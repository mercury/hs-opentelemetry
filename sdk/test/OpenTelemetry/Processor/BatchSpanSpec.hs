{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module OpenTelemetry.Processor.BatchSpanSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently_)
import Control.Monad (forM_, replicateM_, void)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.Text (Text)
import qualified Data.Vector as V
import OpenTelemetry.Exporter.Span (SpanExporter (..))
import OpenTelemetry.Internal.Common.Types (ExportResult (..), FlushResult (..), InstrumentationLibrary, ShutdownResult (..), instrumentationLibrary)
import OpenTelemetry.Processor.Batch.Span
import OpenTelemetry.Trace.Core
import Test.Hspec


-- | An exporter that records every batch it receives, in order.
recordingExporter :: IO (SpanExporter, IORef [HM.HashMap InstrumentationLibrary (V.Vector ImmutableSpan)])
recordingExporter = do
  ref <- newIORef []
  pure
    ( SpanExporter
        { spanExporterExport = \batch -> do
            atomicModifyIORef' ref (\bs -> (batch : bs, ()))
            pure Success
        , spanExporterShutdown = pure ShutdownSuccess
        , spanExporterForceFlush = pure FlushSuccess
        }
    , ref
    )


exportedSpans :: IORef [HM.HashMap InstrumentationLibrary (V.Vector ImmutableSpan)] -> IO [HM.HashMap InstrumentationLibrary (V.Vector ImmutableSpan)]
exportedSpans ref = reverse <$> readIORef ref


spanNames :: HM.HashMap InstrumentationLibrary (V.Vector ImmutableSpan) -> IO (HM.HashMap InstrumentationLibrary [Text])
spanNames = traverse (fmap V.toList . traverse (\s -> hotName <$> readIORef (spanHot s)))


totalSpans :: [HM.HashMap InstrumentationLibrary (V.Vector ImmutableSpan)] -> Int
totalSpans = sum . map (sum . map V.length . HM.elems)


withProvider :: BatchTimeoutConfig -> SpanExporter -> (TracerProvider -> IO a) -> IO a
withProvider cfg exporter act = do
  proc <- batchProcessor cfg exporter
  tp <- createTracerProvider [proc] emptyTracerProviderOptions
  r <- act tp
  void $ shutdownTracerProvider tp Nothing
  pure r


-- Long scheduled delay so tests observe only explicit flushes.
quietConfig :: BatchTimeoutConfig
quietConfig =
  batchTimeoutConfig
    { scheduledDelayMillis = 60_000
    , maxQueueSize = 64
    , maxExportBatchSize = 8
    }


spec :: Spec
spec = describe "BatchSpanProcessor" $ do
  it "groups spans by instrumentation scope and preserves chronological order" $ do
    (exporter, ref) <- recordingExporter
    let libA = instrumentationLibrary "lib-a" "1"
        libB = instrumentationLibrary "lib-b" "2"
    withProvider quietConfig exporter $ \tp -> do
      let ta = makeTracer tp libA tracerOptions
          tb = makeTracer tp libB tracerOptions
      forM_ ["a1", "a2", "a3"] $ \n -> inSpan'' ta n defaultSpanArguments (\_ -> pure ())
      forM_ ["b1", "b2"] $ \n -> inSpan'' tb n defaultSpanArguments (\_ -> pure ())
      void $ forceFlushTracerProvider tp Nothing
    batches <- exportedSpans ref
    case batches of
      [batch] -> do
        names <- spanNames batch
        HM.lookup libA names `shouldBe` Just ["a1", "a2", "a3"]
        HM.lookup libB names `shouldBe` Just ["b1", "b2"]
      _ -> expectationFailure ("expected exactly one batch, got " <> show (length batches))

  it "exports automatically once a full batch has accumulated" $ do
    (exporter, ref) <- recordingExporter
    withProvider quietConfig exporter $ \tp -> do
      let t = makeTracer tp (instrumentationLibrary "lib" "1") tracerOptions
      replicateM_ 8 $ inSpan'' t "s" defaultSpanArguments (\_ -> pure ())
      -- Give the worker a moment to wake on the size signal.
      threadDelay 100_000
      batches <- exportedSpans ref
      totalSpans batches `shouldBe` 8

  it "splits oversized flushes into maxExportBatchSize chunks" $ do
    (exporter, ref) <- recordingExporter
    withProvider quietConfig {maxExportBatchSize = 4, maxQueueSize = 100} exporter $ \tp -> do
      let t = makeTracer tp (instrumentationLibrary "lib" "1") tracerOptions
      replicateM_ 10 $ inSpan'' t "s" defaultSpanArguments (\_ -> pure ())
      void $ forceFlushTracerProvider tp Nothing
    batches <- exportedSpans ref
    totalSpans batches `shouldBe` 10
    forM_ batches $ \b -> sum (map V.length (HM.elems b)) `shouldSatisfy` (<= 4)

  it "drops spans when the queue is full and keeps the rest" $ do
    -- Exporter that blocks until released so the queue cannot drain.
    gate <- newIORef False
    ref <- newIORef []
    let exporter =
          SpanExporter
            { spanExporterExport = \batch -> do
                let waitGate = do
                      ok <- readIORef gate
                      if ok then pure () else threadDelay 1_000 >> waitGate
                waitGate
                atomicModifyIORef' ref (\bs -> (batch : bs, ()))
                pure Success
            , spanExporterShutdown = pure ShutdownSuccess
            , spanExporterForceFlush = pure FlushSuccess
            }
        cfg = quietConfig {maxQueueSize = 16, maxExportBatchSize = 1000}
    proc <- batchProcessor cfg exporter
    tp <- createTracerProvider [proc] emptyTracerProviderOptions
    let t = makeTracer tp (instrumentationLibrary "lib" "1") tracerOptions
    replicateM_ 40 $ inSpan'' t "s" defaultSpanArguments (\_ -> pure ())
    writeIORef gate True
    void $ shutdownTracerProvider tp Nothing
    batches <- exportedSpans ref
    let n = totalSpans batches
    n `shouldSatisfy` (\x -> x >= 16 && x < 40)

  it "flushes everything on shutdown" $ do
    (exporter, ref) <- recordingExporter
    withProvider quietConfig exporter $ \tp -> do
      let t = makeTracer tp (instrumentationLibrary "lib" "1") tracerOptions
      replicateM_ 5 $ inSpan'' t "s" defaultSpanArguments (\_ -> pure ())
    batches <- exportedSpans ref
    totalSpans batches `shouldBe` 5

  it "loses nothing under concurrent producers when the queue is large enough" $ do
    (exporter, ref) <- recordingExporter
    let cfg = batchTimeoutConfig {maxQueueSize = 100_000, maxExportBatchSize = 512, scheduledDelayMillis = 60_000}
    withProvider cfg exporter $ \tp -> do
      let t = makeTracer tp (instrumentationLibrary "lib" "1") tracerOptions
      mapConcurrently_
        (\_ -> replicateM_ 2_000 $ inSpan'' t "s" defaultSpanArguments (\_ -> pure ()))
        [1 .. 8 :: Int]
    batches <- exportedSpans ref
    totalSpans batches `shouldBe` 16_000
