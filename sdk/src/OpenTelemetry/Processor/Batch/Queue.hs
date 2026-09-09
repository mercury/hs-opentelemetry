{-# LANGUAGE BangPatterns #-}

{- |
Module      :  OpenTelemetry.Processor.Batch.Queue
Copyright   :  (c) Ian Duncan, 2026
License     :  BSD-3
Description :  Bounded multi-producer queue shared by the batch processors.
Stability   :  internal

The batch span processor and the batch log processor both use this queue.
Producers run on request threads, so the push must be cheap and must not
make producers wait for each other. The worker thread takes the whole queue
in one step.

= Note [Evaluated-value CAS]

The queue is an 'IORef' updated with 'casReadModifyIORef_' from
"OpenTelemetry.Util". That function computes the new value first and then
does one compare-and-swap. A thread that loses the swap tries again.

'atomicModifyIORef'' is not used here. It stores a thunk in the reference
and forces the thunk after the swap. Under contention the thunk of thread N
refers to the thunk of thread N-1, so each thread waits for the previous
thread to finish its evaluation. Each push then costs a scheduler round
trip. The metrics store in "OpenTelemetry.MeterProvider" uses
'OpenTelemetry.Util.casModifyIORef_' for the same reason.
-}
module OpenTelemetry.Processor.Batch.Queue (
  Pending (..),
  emptyPending,
  pushPending,
  drainPending,
) where

import Data.IORef (IORef)
import OpenTelemetry.Util (casReadModifyIORef_)


-- | Items that wait for export, newest first, with their count.
data Pending a = Pending
  { pendingCount :: {-# UNPACK #-} !Int
  , pendingItems :: ![a]
  }


emptyPending :: Pending a
emptyPending = Pending 0 []
{-# INLINE emptyPending #-}


{- | Push an item, unless the queue is full.

Returns the queue depth after the push, or 'Nothing' if the item was
dropped. See Note [Evaluated-value CAS].
-}
pushPending :: Int -> a -> IORef (Pending a) -> IO (Maybe Int)
pushPending bound x ref = do
  old <- casReadModifyIORef_ ref $ \p@(Pending n xs) ->
    if n >= bound then p else Pending (n + 1) (x : xs)
  let !n = pendingCount old
  pure $! if n >= bound then Nothing else Just (n + 1)
{-# INLINE pushPending #-}


-- | Take all pending items and make the queue empty, in one atomic step.
drainPending :: IORef (Pending a) -> IO (Pending a)
drainPending ref = casReadModifyIORef_ ref (const emptyPending)
{-# INLINE drainPending #-}
