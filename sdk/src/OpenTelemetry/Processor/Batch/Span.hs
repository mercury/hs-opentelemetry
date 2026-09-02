{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}

-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

{- |
 Module      :  OpenTelemetry.Processor.Batch.Span
 Copyright   :  (c) Ian Duncan, 2021
 License     :  BSD-3
 Description :  Performant exporting of spans in time & space-bounded batches.
 Maintainer  :  Ian Duncan
 Stability   :  experimental
 Portability :  non-portable (GHC extensions)

 This is an implementation of the Span Processor which create batches of finished spans and passes the export-friendly span data representations to the configured Exporter.
-}
module OpenTelemetry.Processor.Batch.Span (
  BatchTimeoutConfig (..),
  batchTimeoutConfig,
  batchProcessor,
  -- , BatchProcessorOperations
) where

import Control.Concurrent (rtsSupportsBoundThreads)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Vector (Vector)
import qualified Data.Vector as V
import OpenTelemetry.Exporter.Span (SpanExporter)
import qualified OpenTelemetry.Exporter.Span as SpanExporter
import OpenTelemetry.Internal.Logging (otelLogWarning)
import OpenTelemetry.Processor.Span
import OpenTelemetry.Trace.Core
import OpenTelemetry.Util (casReadModifyIORef_)
import System.Timeout (timeout)


-- | Configurable options for batch exporting frequence and size
data BatchTimeoutConfig = BatchTimeoutConfig
  { maxQueueSize :: Int
  -- ^ The maximum queue size. After the size is reached, spans are dropped.
  , scheduledDelayMillis :: Int
  {- ^ The delay interval in milliseconds between two consective exports.
  The default value is 5000.
  -}
  , exportTimeoutMillis :: Int
  {- ^ How long the export can run before it is cancelled.
  The default value is 30000.
  -}
  , maxExportBatchSize :: Int
  {- ^ The maximum batch size of every export. It must be
  smaller or equal to 'maxQueueSize'. The default value is 512.
  -}
  }
  deriving (Show)


-- | Default configuration values
batchTimeoutConfig :: BatchTimeoutConfig
batchTimeoutConfig =
  BatchTimeoutConfig
    { maxQueueSize = 2048
    , scheduledDelayMillis = 5000
    , exportTimeoutMillis = 30000
    , maxExportBatchSize = 512
    }


-- type BatchProcessorOperations = ()

--  A multi-producer single-consumer green/blue buffer.
-- Write requests that cannot fit in the live chunk will be dropped
--
-- TODO, would be cool to use AtomicCounters for this if possible
-- data GreenBlueBuffer a = GreenBlueBuffer
--   { gbReadSection :: !(TVar Word)
--   , gbWriteGreenOrBlue :: !(TVar Word)
--   , gbPendingWrites :: !(TVar Word)
--   , gbSectionSize :: !Int
--   , gbVector :: !(M.IOVector a)
--   }

{- brainstorm: Single Word64 state sketch

  63 (high bit): green or blue
  32-62: read section
  0-32: write count
-}

{-

Green
    512       512       512       512
\|---------|---------|---------|---------|
     0         1         2         3

Blue
    512       512       512       512
\|---------|---------|---------|---------|
     0         1         2         3

The current read section denotes one chunk of length gbSize, which gets flushed
to the span exporter. Once the vector has been copied for export, gbReadSection
will be incremented.

-}

-- newGreenBlueBuffer
--   :: Int  --  Max queue size (2048)
--   -> Int  --  Export batch size (512)
--   -> IO (GreenBlueBuffer a)
-- newGreenBlueBuffer maxQueueSize batchSize = do
--   let logBase2 = finiteBitSize maxQueueSize - 1 - countLeadingZeros maxQueueSize

--   let closestFittingPowerOfTwo = 2 * if (1 `shiftL` logBase2) == maxQueueSize
--         then maxQueueSize
--         else 1 `shiftL` (logBase2 + 1)

--   readSection <- newTVarIO 0
--   writeSection <- newTVarIO 0
--   writeCount <- newTVarIO 0
--   buf <- M.new closestFittingPowerOfTwo
--   pure $ GreenBlueBuffer
--     { gbSize = maxQueueSize
--     , gbVector = buf
--     , gbReadSection = readSection
--     , gbPendingWrites = writeCount
--     }

-- isEmpty :: GreenBlueBuffer a -> STM Bool
-- isEmpty = do
--   c <- readTVar gbPendingWrites
--   pure (c == 0)

-- data InsertResult = ValueDropped | ValueInserted

-- tryInsert :: GreenBlueBuffer a -> a -> IO InsertResult
-- tryInsert GreenBlueBuffer{..} x = atomically $ do
--   c <- readTVar gbPendingWrites
--   if c == gbMaxLength
--     then pure ValueDropped
--     else do
--       greenOrBlue <- readTVar gbWriteGreenOrBlue
--       let i = c + ((M.length gbVector `shiftR` 1) `shiftL` (greenOrBlue `mod` 2))
--       M.write gbVector i x
--       writeTVar gbPendingWrites (c + 1)
--       pure ValueInserted

-- Caution, single writer means that this can't be called concurrently
-- consumeChunk :: GreenBlueBuffer a -> IO (V.Vector a)
-- consumeChunk GreenBlueBuffer{..} = atomically $ do
--   r <- readTVar gbReadSection
--   w <- readTVar gbWriteSection
--   c <- readTVar gbPendingWrites
--   when (r == w) $ do
--     modifyTVar gbWriteSection (+ 1)
--     setTVar gbPendingWrites 0
--   -- TODO slice and freeze appropriate section
-- M.slice (gbSectionSize * (r .&. gbSectionMask)

{- | Spans waiting for export, newest first.

The producer side (every 'endSpan' on every request thread) only conses
onto this list and bumps the count.  Grouping by instrumentation scope
happens on the single worker thread in 'groupByScope', so the
compare-and-swap window on the hot path is one small allocation and does
not hash 'InstrumentationLibrary'.
-}
data Pending = Pending
  { pendingCount :: {-# UNPACK #-} !Int
  , pendingSpans :: ![ImmutableSpan]
  }


emptyPending :: Pending
emptyPending = Pending 0 []


{- | Push a span unless the queue is at capacity.  Returns the queue
depth after the push, or 'Nothing' when the span was dropped.

Uses an evaluated-value CAS rather than 'atomicModifyIORef'', which
installs a thunk and makes concurrent callers block on each other's
blackholes.  Under contention losers simply retry a very cheap step.
-}
pushPending :: Int -> ImmutableSpan -> IORef Pending -> IO (Maybe Int)
pushPending bound s ref = do
  old <- casReadModifyIORef_ ref $ \p@(Pending n xs) ->
    if n >= bound then p else Pending (n + 1) (s : xs)
  let !n = pendingCount old
  pure $! if n >= bound then Nothing else Just (n + 1)


-- | Atomically take everything that is pending and reset the queue.
drainPending :: IORef Pending -> IO [ImmutableSpan]
drainPending ref = pendingSpans <$> casReadModifyIORef_ ref (const emptyPending)


{- | Group a newest-first span list by instrumentation scope, preserving
chronological order within each scope.  Runs on the worker thread only.
-}
groupByScope :: [ImmutableSpan] -> HashMap InstrumentationLibrary (Vector ImmutableSpan)
groupByScope spans =
  (\(n, xs) -> V.fromListN n xs)
    <$> foldl'
      (\m s -> HashMap.alter (Just . step s) (tracerName (spanTracer s)) m)
      HashMap.empty
      spans
  where
    step s Nothing = (1 :: Int, [s])
    step s (Just (n, xs)) = (n + 1, s : xs)


data ProcessorMessage = ScheduledFlush | MaxExportFlush | FlushRequested | Shutdown


-- note: [Unmasking Asyncs]
--
-- It is possible to create unkillable asyncs. Behold:
--
-- ```
-- a <- uninterruptibleMask_ $ do
--     async $ do
--         forever $ do
--             threadDelay 10_000
--             putStrLn "still alive"
-- cancel a
-- ```
--
-- The prior code block will never successfully cancel `a` and will be
-- blocked forever. The reason is that `cancel` sends an async exception to
-- the thread performing the action, but the `uninterruptibleMask` state is
-- inherited by the forked thread. This means that *no async exceptions*
-- can reach it, and `cancel` will therefore run forever.
--
-- This also affects `timeout`, which uses an async exception to kill the
-- running job. If the action is done in an uninterruptible masked state,
-- then the timeout will not successfully kill the running action.

{- |
 The batch processor accepts spans and places them into batches. Batching helps better compress the data and reduce the number of outgoing connections
 required to transmit the data. This processor supports both size and time based batching.

 NOTE: this function requires the program be compiled with the @-threaded@ GHC
 option and will throw an error if this is not the case.
-}
batchProcessor :: (MonadIO m) => BatchTimeoutConfig -> SpanExporter -> m SpanProcessor
batchProcessor BatchTimeoutConfig {..} exporter = liftIO $ do
  unless rtsSupportsBoundThreads $ error "The hs-opentelemetry batch processor does not work without the -threaded GHC flag!"
  batch <- newIORef emptyPending
  droppedRef <- newIORef (0 :: Int)
  warnedRef <- newIORef False
  workSignal <- newEmptyTMVarIO
  shutdownSignal <- newEmptyTMVarIO
  flushRequestSignal <- newEmptyTMVarIO
  flushDoneSignal <- newEmptyTMVarIO
  let timeoutMicros = millisToMicros exportTimeoutMillis

  let publish batchToProcess = do
        mResult <-
          timeout timeoutMicros $
            mask_ $
              SpanExporter.spanExporterExport exporter batchToProcess
        pure $ case mResult of
          Nothing -> SpanExporter.Failure Nothing
          Just r -> r

  -- Split a batch map into a chunk of at most n items and a remainder.
  let splitBatch n m = go n (HashMap.toList m) [] []
        where
          go _ [] chunkAcc restAcc = (HashMap.fromList chunkAcc, HashMap.fromList restAcc)
          go 0 remaining chunkAcc restAcc = (HashMap.fromList chunkAcc, HashMap.fromList (restAcc ++ remaining))
          go remaining ((lib, vec) : rest) chunkAcc restAcc
            | V.length vec <= remaining =
                go (remaining - V.length vec) rest ((lib, vec) : chunkAcc) restAcc
            | otherwise =
                let (front, back) = V.splitAt remaining vec
                in go 0 rest ((lib, front) : chunkAcc) ((lib, back) : restAcc)

  let publishBounded batchMap
        | HashMap.null batchMap = pure SpanExporter.Success
        | sum (V.length <$> HashMap.elems batchMap) <= maxExportBatchSize =
            publish batchMap
        | otherwise = do
            let (chunk, rest) = splitBatch maxExportBatchSize batchMap
            res <- publish chunk
            case res of
              SpanExporter.Failure _ -> pure res
              SpanExporter.Success -> publishBounded rest

  let takeBatch = groupByScope <$> drainPending batch

  let flushQueueImmediately ret = do
        batchToProcess <- takeBatch
        if null batchToProcess
          then pure ret
          else do
            ret' <- publishBounded batchToProcess
            flushQueueImmediately ret'

  -- Shutdown and FlushRequested are tried before work signals so they
  -- cannot be starved under sustained high throughput.
  let waiting = do
        delay <- registerDelay (millisToMicros scheduledDelayMillis)
        atomically $
          msum
            [ Shutdown <$ takeTMVar shutdownSignal
            , FlushRequested <$ takeTMVar flushRequestSignal
            , MaxExportFlush <$ takeTMVar workSignal
            , ScheduledFlush <$ do
                continue <- readTVar delay
                check continue
            ]

  let workerAction = do
        req <- waiting
        batchToProcess <- takeBatch
        res <- publishBounded batchToProcess

        case req of
          Shutdown ->
            flushQueueImmediately res
          FlushRequested -> do
            _ <- flushQueueImmediately res
            atomically $ putTMVar flushDoneSignal ()
            workerAction
          _ ->
            workerAction
  -- see note [Unmasking Asyncs]
  worker <- asyncWithUnmask $ \unmask -> unmask workerAction

  pure $
    SpanProcessor
      { spanProcessorOnStart = \_ _ -> pure ()
      , spanProcessorOnEnd = \s -> do
          mDepth <- pushPending maxQueueSize s batch
          case mDepth of
            Nothing -> do
              warnOnDrop droppedRef warnedRef maxQueueSize "BatchSpanProcessor"
              void $ atomically $ tryPutTMVar workSignal ()
            Just depth ->
              -- Wake the worker once per full batch rather than on every
              -- span past the threshold; the STM transaction is far more
              -- expensive than the push itself and touches a shared TVar.
              when (depth `rem` maxExportBatchSize == 0) $
                void $
                  atomically $
                    tryPutTMVar workSignal ()
      , spanProcessorForceFlush = do
          atomically $ putTMVar flushRequestSignal ()
          atomically $ takeTMVar flushDoneSignal
          SpanExporter.spanExporterForceFlush exporter
      , -- TODO where to call restore, if anywhere?
        spanProcessorShutdown =
          mask $ \_restore -> do
            -- we use interruptible `mask` because the shutdown action is
            -- likely to happen inside of a `finally` or `bracket`. the
            -- @safe-exceptions@ pattern (followed by unliftio as well)
            -- will use uninterruptibleMask in an exception cleanup. the
            -- uninterruptibleMask state means that the `timeout` call
            -- below will never exit, because `wait worker` will be in the
            -- `uninterruptibleMasked` state, and the timeout async
            -- exception will not be delivered.
            --
            -- see note [Unmasking Asyncs]

            -- flush remaining messages and signal the worker to shutdown
            void $ atomically $ putTMVar shutdownSignal ()

            -- gracefully wait for the worker to stop. we may be in
            -- a `bracket` or responding to an async exception, so we
            -- must be very careful not to wait too long. the following
            -- STM action will block, so we'll be susceptible to an async
            -- exception.
            delay <- registerDelay (millisToMicros exportTimeoutMillis)
            shutdownResult <-
              atomically $
                msum
                  [ Just <$> waitCatchSTM worker
                  , Nothing <$ do
                      shouldStop <- readTVar delay
                      check shouldStop
                  ]

            -- make sure the worker comes down if we timed out.
            cancel worker
            -- OTel spec: Processor.Shutdown MUST shut down the exporter
            _ <- SpanExporter.spanExporterShutdown exporter

            pure $ case shutdownResult of
              Nothing ->
                ShutdownTimeout
              Just er ->
                case er of
                  Left _ ->
                    ShutdownFailure
                  Right _ ->
                    ShutdownSuccess
      }
  where
    millisToMicros = (* 1000)


{-
buffer <- newGreenBlueBuffer _ _
batchProcessorAction <- async $ forever $ do
  -- It would be nice to do an immediate send when possible
  chunk <- if (sendDelay == 0)
    else consumeChunk
    then threadDelay sendDelay >> consumeChunk
  timeout _ $ export exporter chunk
pure $ Processor
  { onStart = \_ _ -> pure ()
  , onEnd = \s -> void $ tryInsert buffer s
  , shutdown = do
      gracefullyShutdownBatchProcessor

  , forceFlush = pure ()
  }
where
  sendDelay = scheduledDelayMilis * 1_000
-}

warnOnDrop :: IORef Int -> IORef Bool -> Int -> String -> IO ()
warnOnDrop droppedRef warnedRef capacity processorName = do
  n <- atomicModifyIORef' droppedRef (\c -> let c' = c + 1 in (c', c'))
  alreadyWarned <- atomicModifyIORef' warnedRef (\w -> (True, w))
  unless alreadyWarned $
    otelLogWarning $
      processorName
        <> ": queue full (capacity "
        <> show capacity
        <> "), dropping span. Total dropped so far: "
        <> show n
