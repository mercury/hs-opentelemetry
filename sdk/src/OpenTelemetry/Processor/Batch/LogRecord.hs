{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}

module OpenTelemetry.Processor.Batch.LogRecord (
  BatchLogRecordProcessorConfig (..),
  defaultBatchLogRecordProcessorConfig,
  batchLogRecordProcessor,
) where

import Control.Concurrent (rtsSupportsBoundThreads)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Vector (Vector)
import qualified Data.Vector as V
import OpenTelemetry.Internal.Common.Types (ExportResult (..), ShutdownResult (..))
import OpenTelemetry.Internal.Log.Types
import OpenTelemetry.Internal.Logging (otelLogWarning)
import OpenTelemetry.Util (casReadModifyIORef_)
import System.Timeout (timeout)


data BatchLogRecordProcessorConfig = BatchLogRecordProcessorConfig
  { batchLogExporter :: !LogRecordExporter
  , batchLogMaxQueueSize :: !Int
  , batchLogScheduledDelayMillis :: !Int
  , batchLogExportTimeoutMillis :: !Int
  , batchLogMaxExportBatchSize :: !Int
  }


defaultBatchLogRecordProcessorConfig :: LogRecordExporter -> BatchLogRecordProcessorConfig
defaultBatchLogRecordProcessorConfig e =
  BatchLogRecordProcessorConfig
    { batchLogExporter = e
    , batchLogMaxQueueSize = 2048
    , batchLogScheduledDelayMillis = 1000
    , batchLogExportTimeoutMillis = 30000
    , batchLogMaxExportBatchSize = 512
    }


{- | Log records that wait for export, newest first.

See the 'Pending' type in "OpenTelemetry.Processor.Batch.Span" for the
reason this is a plain list behind an evaluated-value compare-and-swap.
-}
data Pending = Pending
  { pendingCount :: {-# UNPACK #-} !Int
  , pendingRecords :: ![ReadableLogRecord]
  }


emptyPending :: Pending
emptyPending = Pending 0 []


-- | Returns the queue depth after the push, or 'Nothing' if the record was dropped.
pushPending :: Int -> ReadableLogRecord -> IORef Pending -> IO (Maybe Int)
pushPending bound lr ref = do
  old <- casReadModifyIORef_ ref $ \p@(Pending n xs) ->
    if n >= bound then p else Pending (n + 1) (lr : xs)
  let !n = pendingCount old
  pure $! if n >= bound then Nothing else Just (n + 1)


-- | Take all pending records, oldest first, and make the queue empty, in one atomic step.
drainPending :: IORef Pending -> IO (Vector ReadableLogRecord)
drainPending ref = do
  p <- casReadModifyIORef_ ref (const emptyPending)
  pure $! V.fromListN (pendingCount p) (reverse (pendingRecords p))


data ProcessorMessage = ScheduledFlush | MaxExportFlush | FlushRequested | Shutdown


batchLogRecordProcessor :: (MonadIO m) => BatchLogRecordProcessorConfig -> m LogRecordProcessor
batchLogRecordProcessor BatchLogRecordProcessorConfig {..} = liftIO $ do
  unless rtsSupportsBoundThreads $
    throwIO (userError "The threaded runtime is required for the batch log record processor")
  batch <- newIORef emptyPending
  droppedRef <- newIORef (0 :: Int)
  warnedRef <- newIORef False
  workSignal <- newEmptyTMVarIO
  shutdownSignal <- newEmptyTMVarIO
  flushRequestSignal <- newEmptyTMVarIO
  flushDoneSignal <- newEmptyTMVarIO

  let timeoutMicros = millisToMicros batchLogExportTimeoutMillis

  let publish batchToExport = do
        mResult <-
          timeout timeoutMicros $
            mask_ $
              logRecordExporterExport batchLogExporter batchToExport
        pure $ case mResult of
          Nothing -> Failure Nothing
          Just r -> r

  let publishBounded batchToExport
        | V.null batchToExport = pure Success
        | V.length batchToExport <= batchLogMaxExportBatchSize =
            publish batchToExport
        | otherwise = do
            let (chunk, rest) = V.splitAt batchLogMaxExportBatchSize batchToExport
            res <- publish chunk
            case res of
              Failure _ -> pure res
              Success -> publishBounded rest

  let flushQueueImmediately ret = do
        batchToExport <- drainPending batch
        if V.null batchToExport
          then pure ret
          else do
            ret' <- publishBounded batchToExport
            flushQueueImmediately ret'

  -- Shutdown and FlushRequested are tried before work signals so they
  -- cannot be starved under sustained high throughput.
  let waiting = do
        delay <- registerDelay (millisToMicros batchLogScheduledDelayMillis)
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
        batchToExport <- drainPending batch
        res <- publishBounded batchToExport
        case req of
          Shutdown -> flushQueueImmediately res
          FlushRequested -> do
            _ <- flushQueueImmediately res
            atomically $ putTMVar flushDoneSignal ()
            workerAction
          _ -> workerAction

  worker <- asyncWithUnmask $ \unmask -> unmask workerAction

  pure
    LogRecordProcessor
      { logRecordProcessorOnEmit = \lr _ctxt -> do
          readable <- mkReadableLogRecord lr
          mDepth <- pushPending batchLogMaxQueueSize readable batch
          case mDepth of
            Nothing -> do
              warnOnDrop droppedRef warnedRef batchLogMaxQueueSize "BatchLogRecordProcessor"
              void $ atomically $ tryPutTMVar workSignal ()
            Just depth ->
              when (depth `rem` batchLogMaxExportBatchSize == 0) $
                void $
                  atomically $
                    tryPutTMVar workSignal ()
      , logRecordProcessorForceFlush = do
          atomically $ putTMVar flushRequestSignal ()
          atomically $ takeTMVar flushDoneSignal
          logRecordExporterForceFlush batchLogExporter
      , logRecordProcessorShutdown =
          mask $ \_restore -> do
            void $ atomically $ putTMVar shutdownSignal ()
            delay <- registerDelay (millisToMicros batchLogExportTimeoutMillis)
            shutdownResult <-
              atomically $
                msum
                  [ Just <$> waitCatchSTM worker
                  , Nothing <$ do
                      shouldStop <- readTVar delay
                      check shouldStop
                  ]
            cancel worker
            logRecordExporterShutdown batchLogExporter
            pure $ case shutdownResult of
              Nothing -> ShutdownTimeout
              Just (Left _) -> ShutdownFailure
              Just (Right _) -> ShutdownSuccess
      }
  where
    millisToMicros = (* 1000)


warnOnDrop :: IORef Int -> IORef Bool -> Int -> String -> IO ()
warnOnDrop droppedRef warnedRef capacity processorName = do
  n <- atomicModifyIORef' droppedRef (\c -> let c' = c + 1 in (c', c'))
  alreadyWarned <- atomicModifyIORef' warnedRef (\w -> (True, w))
  unless alreadyWarned $
    otelLogWarning $
      processorName
        <> ": queue full (capacity "
        <> show capacity
        <> "), dropping log record. Total dropped so far: "
        <> show n
