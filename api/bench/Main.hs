{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Exception (evaluate)
import Control.Monad (forM_, void)
import qualified Data.HashMap.Strict as H
import Data.IORef
import qualified Data.Text as T
import GHC.Clock (getMonotonicTimeNSec)
import OpenTelemetry.Attributes (defaultAttributeLimits, emptyAttributes)
import qualified OpenTelemetry.Attributes as A
import OpenTelemetry.Context (empty, insertSpan, lookupSpan)
import OpenTelemetry.Context.ThreadLocal (adjustContext, attachContext, getContext)
import OpenTelemetry.Internal.AtomicCounter
import OpenTelemetry.Processor.Span (FlushResult (..), ShutdownResult (..), SpanProcessor (..))
import OpenTelemetry.Trace.Core
import OpenTelemetry.Trace.Id (newSpanId, newTraceAndSpanId, newTraceId)
import OpenTelemetry.Trace.Id.Generator (IdGenerator (..))
import Test.Tasty.Bench


realOptions :: TracerProviderOptions
realOptions =
  emptyTracerProviderOptions
    { tracerProviderOptionsIdGenerator = DefaultIdGenerator
    }


main :: IO ()
main = do
  noopTp <- createTracerProvider [] emptyTracerProviderOptions
  let noopTracer = makeTracer noopTp (InstrumentationLibrary "bench" "1.0" "" emptyAttributes) tracerOptions

  dummyProcessor <- mkCountingProcessor
  activeTp <- createTracerProvider [dummyProcessor] realOptions
  let activeTracer = makeTracer activeTp (InstrumentationLibrary "bench" "1.0" "" emptyAttributes) tracerOptions

  calibRef <- newIORef ()

  -- The inputs for the larger attribute counts are built here, one time.
  -- The benchmarks then measure only the span or Attributes operation.
  map10 <- evaluate $ mkAttrMap 10
  map20 <- evaluate $ mkAttrMap 20
  map100 <- evaluate $ mkAttrMap 100
  let builder10 = mkAttrBuilder 10
      builder20 = mkAttrBuilder 20
      builder100 = mkAttrBuilder 100
  keys10 <- evaluate $ mkKeys 10
  keys20 <- evaluate $ mkKeys 20
  keys100 <- evaluate $ mkKeys 100
  map5 <- evaluate $ mkAttrMap 5
  -- A populated base with keys that do not collide with the batches above.
  base5 <- evaluate $ A.unsafeAttributesFromMap defaultAttributeLimits (mkAttrMapFrom 1001 5)
  base100 <- evaluate $ A.unsafeAttributesFromMap defaultAttributeLimits (mkAttrMapFrom 1001 100)

  defaultMain
    [ bgroup
        "calibration"
        [ bench "noop IO" $ whnfIO (pure ())
        , bench "IORef read" $ whnfIO (readIORef calibRef)
        , bench "IORef write" $ whnfIO (writeIORef calibRef ())
        , bench "atomicModifyIORef'" $ whnfIO (atomicModifyIORef' calibRef (\x -> (x, ())))
        , bench "getMonotonicTimeNSec" $ whnfIO getMonotonicTimeNSec
        ]
    , bgroup
        "createSpan"
        [ bench "no-op (no processors)" $
            whnfIO $
              createSpan noopTracer empty "bench-span" defaultSpanArguments
        , bench "active (with processor)" $
            whnfIO $
              createSpan activeTracer empty "bench-span" defaultSpanArguments
        , bench "active + parent context" $ whnfIO $ do
            parent <- createSpan activeTracer empty "parent" defaultSpanArguments
            let ctx = insertSpan parent empty
            createSpan activeTracer ctx "child" defaultSpanArguments
        ]
    , bgroup
        "endSpan"
        [ bench "no-op span" $ whnfIO $ do
            s <- createSpan noopTracer empty "s" defaultSpanArguments
            endSpan s Nothing
        , bench "active span" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            endSpan s Nothing
        ]
    , bgroup
        "isRecording"
        [ bench "Dropped" $ whnfIO $ do
            s <- createSpan noopTracer empty "s" defaultSpanArguments
            isRecording s
        , bench "live Span" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            isRecording s
        ]
    , bgroup
        "addAttribute"
        [ bench "on Dropped span" $ whnfIO $ do
            s <- createSpan noopTracer empty "s" defaultSpanArguments
            addAttribute s "key" ("value" :: T.Text)
        , bench "on live span (1 attr)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttribute s "key" ("value" :: T.Text)
        , bench "on live span (10 attrs sequential)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttribute s "k1" ("v" :: T.Text)
            addAttribute s "k2" ("v" :: T.Text)
            addAttribute s "k3" ("v" :: T.Text)
            addAttribute s "k4" ("v" :: T.Text)
            addAttribute s "k5" ("v" :: T.Text)
            addAttribute s "k6" ("v" :: T.Text)
            addAttribute s "k7" ("v" :: T.Text)
            addAttribute s "k8" ("v" :: T.Text)
            addAttribute s "k9" ("v" :: T.Text)
            addAttribute s "k10" ("v" :: T.Text)
        , -- Each attribute costs one CAS and one insert into a map that grows.
          bench "on live span (20 attrs sequential)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            forM_ keys20 $ \k -> addAttribute s k ("v" :: T.Text)
        , bench "on live span (100 attrs sequential)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            forM_ keys100 $ \k -> addAttribute s k ("v" :: T.Text)
        ]
    , bgroup
        "addAttributes-batch"
        [ bench "H.fromList 10 attrs" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes s $
              H.fromList
                [ ("k1", "v")
                , ("k2", "v")
                , ("k3", "v")
                , ("k4", "v")
                , ("k5", "v")
                , ("k6", "v")
                , ("k7", "v")
                , ("k8", "v")
                , ("k9", "v")
                , ("k10", "v")
                ]
        , bench "AttrsBuilder 10 attrs" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes' s $
              A.attr "k1" ("v" :: T.Text)
                <> A.attr "k2" ("v" :: T.Text)
                <> A.attr "k3" ("v" :: T.Text)
                <> A.attr "k4" ("v" :: T.Text)
                <> A.attr "k5" ("v" :: T.Text)
                <> A.attr "k6" ("v" :: T.Text)
                <> A.attr "k7" ("v" :: T.Text)
                <> A.attr "k8" ("v" :: T.Text)
                <> A.attr "k9" ("v" :: T.Text)
                <> A.attr "k10" ("v" :: T.Text)
        , bench "H.fromList 3 attrs" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes s $
              H.fromList
                [ ("method", A.toAttribute ("GET" :: T.Text))
                , ("url", A.toAttribute ("https://example.com/api" :: T.Text))
                , ("status", A.toAttribute (200 :: Int))
                ]
        , bench "AttrsBuilder 3 attrs" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes' s $
              A.attr "method" ("GET" :: T.Text)
                <> A.attr "url" ("https://example.com/api" :: T.Text)
                <> A.attr "status" (200 :: Int)
        , -- The inputs below are prepared. These cases measure the merge into the span.
          bench "H.fromList 10 attrs (pre-built)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes s map10
        , bench "H.fromList 20 attrs (pre-built)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes s map20
        , bench "H.fromList 100 attrs (pre-built)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes s map100
        , bench "AttrsBuilder 10 attrs (pre-built)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes' s builder10
        , bench "AttrsBuilder 20 attrs (pre-built)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes' s builder20
        , bench "AttrsBuilder 100 attrs (pre-built)" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            addAttributes' s builder100
        ]
    , bgroup
        "addAttributes-vs-legacy"
        -- Same binary, same inputs: the merge in this branch against the
        -- per-key fold it replaced. Both return the merged map in WHNF.
        [ bench "legacy, 5 onto empty" $ whnf (legacyMerge defaultAttributeLimits emptyAttributes) map5
        , bench "new, 5 onto empty" $ whnf (newMerge defaultAttributeLimits emptyAttributes) map5
        , bench "legacy, 20 onto empty" $ whnf (legacyMerge defaultAttributeLimits emptyAttributes) map20
        , bench "new, 20 onto empty" $ whnf (newMerge defaultAttributeLimits emptyAttributes) map20
        , bench "legacy, 100 onto empty" $ whnf (legacyMerge defaultAttributeLimits emptyAttributes) map100
        , bench "new, 100 onto empty" $ whnf (newMerge defaultAttributeLimits emptyAttributes) map100
        , bench "legacy, 5 onto 5" $ whnf (legacyMerge defaultAttributeLimits base5) map5
        , bench "new, 5 onto 5" $ whnf (newMerge defaultAttributeLimits base5) map5
        , bench "legacy, 20 onto 5" $ whnf (legacyMerge defaultAttributeLimits base5) map20
        , bench "new, 20 onto 5" $ whnf (newMerge defaultAttributeLimits base5) map20
        , bench "legacy, 100 onto 5" $ whnf (legacyMerge defaultAttributeLimits base5) map100
        , bench "new, 100 onto 5" $ whnf (newMerge defaultAttributeLimits base5) map100
        , bench "legacy, 5 onto 100" $ whnf (legacyMerge defaultAttributeLimits base100) map5
        , bench "new, 5 onto 100" $ whnf (newMerge defaultAttributeLimits base100) map5
        ]
    , bgroup
        "createSpan-initial-attrs"
        -- Attributes in SpanArguments go through unsafeAttributesFromMap and
        -- one insert for thread.id when the span is created.
        [ bench "10 attrs" $
            whnfIO $
              createSpan activeTracer empty "s" defaultSpanArguments {attributes = map10}
        , bench "20 attrs" $
            whnfIO $
              createSpan activeTracer empty "s" defaultSpanArguments {attributes = map20}
        , bench "100 attrs" $
            whnfIO $
              createSpan activeTracer empty "s" defaultSpanArguments {attributes = map100}
        , bench "100 attrs, Dropped (no processors)" $
            whnfIO $
              createSpan noopTracer empty "s" defaultSpanArguments {attributes = map100}
        ]
    , bgroup
        "Attributes-pure"
        [ bench "addAttribute x1" $
            whnf
              (\a -> A.addAttribute defaultAttributeLimits a "key" ("val" :: T.Text))
              emptyAttributes
        , bench "addAttribute x10 (same key)" $
            whnf
              ( \a ->
                  let go !acc i =
                        if i > (10 :: Int)
                          then acc
                          else go (A.addAttribute defaultAttributeLimits acc "key" ("val" :: T.Text)) (i + 1)
                  in go a 1
              )
              emptyAttributes
        , bench "addAttribute x10 (distinct keys)" $
            whnf
              ( \a ->
                  let go !acc i =
                        if i > (10 :: Int)
                          then acc
                          else go (A.addAttribute defaultAttributeLimits acc (T.pack $ "key" <> show i) ("val" :: T.Text)) (i + 1)
                  in go a 1
              )
              emptyAttributes
        , bench "addAttributes (HashMap) x5" $
            whnf
              ( \a ->
                  A.addAttributes
                    defaultAttributeLimits
                    a
                    (H.fromList [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5")] :: H.HashMap T.Text A.Attribute)
              )
              emptyAttributes
        , bench "addAttributesFromBuilder x5" $
            whnf
              ( \a ->
                  A.addAttributesFromBuilder
                    defaultAttributeLimits
                    a
                    ( A.attr "k1" ("v1" :: T.Text)
                        <> A.attr "k2" ("v2" :: T.Text)
                        <> A.attr "k3" ("v3" :: T.Text)
                        <> A.attr "k4" ("v4" :: T.Text)
                        <> A.attr "k5" ("v5" :: T.Text)
                    )
              )
              emptyAttributes
        , bench "addAttribute x20 (distinct keys, pre-built)" $
            whnf (\ks -> foldl (\acc k -> A.addAttribute defaultAttributeLimits acc k ("v" :: T.Text)) emptyAttributes ks) keys20
        , bench "addAttribute x100 (distinct keys, pre-built)" $
            whnf (\ks -> foldl (\acc k -> A.addAttribute defaultAttributeLimits acc k ("v" :: T.Text)) emptyAttributes ks) keys100
        , bench "addAttributes (HashMap) x20" $
            whnf (A.addAttributes defaultAttributeLimits emptyAttributes) map20
        , bench "addAttributes (HashMap) x100" $
            whnf (A.addAttributes defaultAttributeLimits emptyAttributes) map100
        , bench "unsafeAttributesFromMap x100" $
            whnf (A.unsafeAttributesFromMap defaultAttributeLimits) map100
        , bench "addAttributesFromBuilder x100" $
            whnf (A.addAttributesFromBuilder defaultAttributeLimits emptyAttributes) builder100
        , -- 100 existing keys plus 100 new keys is above the default limit of
          -- 128. The batch drops 72 of the new keys and counts them.
          bench "addAttributes (HashMap) x100 onto 100 (hits limit)" $
            whnf (A.addAttributes defaultAttributeLimits (A.unsafeAttributesFromMap defaultAttributeLimits map100)) (mkAttrMapFrom 101 100)
        ]
    , bgroup
        "context"
        [ bench "getContext" $ whnfIO getContext
        , bench "attachContext + getContext" $ whnfIO $ do
            _ <- attachContext empty
            getContext
        , bench "adjustContext (insertSpan)" $ whnfIO $ do
            s <- createSpan noopTracer empty "s" defaultSpanArguments
            adjustContext (insertSpan s)
        , bench "lookupSpan" $ whnf lookupSpan empty
        ]
    , bgroup
        "inSpan"
        [ bench "no-op tracer" $
            whnfIO $
              inSpan noopTracer "bench" defaultSpanArguments (pure ())
        , bench "active tracer" $
            whnfIO $
              inSpan activeTracer "bench" defaultSpanArguments (pure ())
        , bench "no-op (skip callerAttributes)" $
            whnfIO $
              inSpan'' noopTracer "bench" defaultSpanArguments (const $ pure ())
        , bench "active (skip callerAttributes)" $
            whnfIO $
              inSpan'' activeTracer "bench" defaultSpanArguments (const $ pure ())
        ]
    , bgroup
        "getSpanContext"
        [ bench "Dropped" $ whnfIO $ do
            s <- createSpan noopTracer empty "s" defaultSpanArguments
            getSpanContext s
        , bench "live Span" $ whnfIO $ do
            s <- createSpan activeTracer empty "s" defaultSpanArguments
            getSpanContext s
        ]
    , bgroup "realistic" $
        let httpSpan tracer = inSpan'' tracer "GET /api/users" defaultSpanArguments $ \s -> do
              addAttribute s ("http.method" :: T.Text) ("GET" :: T.Text)
              addAttribute s ("http.url" :: T.Text) ("https://example.com/api/users" :: T.Text)
              addAttribute s ("http.status_code" :: T.Text) (200 :: Int)
              pure ()
            dbSpan tracer = inSpan'' tracer "SELECT users" defaultSpanArguments {kind = Client} $ \s -> do
              addAttribute s ("db.system" :: T.Text) ("postgresql" :: T.Text)
              addAttribute s ("db.statement" :: T.Text) ("SELECT * FROM users WHERE id = $1" :: T.Text)
              addAttribute s ("db.name" :: T.Text) ("mydb" :: T.Text)
              addAttribute s ("db.operation" :: T.Text) ("SELECT" :: T.Text)
              addAttribute s ("db.sql.table" :: T.Text) ("users" :: T.Text)
              pure ()
            spanWithEvents tracer = inSpan'' tracer "process" defaultSpanArguments $ \s -> do
              addEvent
                s
                NewEvent
                  { newEventName = "item.processed"
                  , newEventAttributes = H.fromList [("item.id", A.toAttribute ("abc" :: T.Text))]
                  , newEventTimestamp = Nothing
                  }
              addEvent
                s
                NewEvent
                  { newEventName = "item.validated"
                  , newEventAttributes = H.fromList [("valid", A.toAttribute True)]
                  , newEventTimestamp = Nothing
                  }
              pure ()
            nestedSpans tracer = inSpan'' tracer "parent" defaultSpanArguments $ \_ ->
              inSpan'' tracer "child" defaultSpanArguments $ \_ ->
                inSpan'' tracer "grandchild" defaultSpanArguments $ \_ ->
                  pure ()
            heavySpan tracer = inSpan'' tracer "heavy" defaultSpanArguments $ \s -> do
              addAttribute s ("k1" :: T.Text) ("v" :: T.Text)
              addAttribute s ("k2" :: T.Text) ("v" :: T.Text)
              addAttribute s ("k3" :: T.Text) ("v" :: T.Text)
              addAttribute s ("k4" :: T.Text) ("v" :: T.Text)
              addAttribute s ("k5" :: T.Text) ("v" :: T.Text)
              setStatus s (Error "something broke")
              addEvent
                s
                NewEvent
                  { newEventName = "exception"
                  , newEventAttributes =
                      H.fromList
                        [ ("exception.type", A.toAttribute ("IOException" :: T.Text))
                        , ("exception.message", A.toAttribute ("file not found" :: T.Text))
                        ]
                  , newEventTimestamp = Nothing
                  }
              pure ()
            bareSpan tracer = inSpan'' tracer "bare" defaultSpanArguments $ \_ -> pure ()
        in [ bench "bare span (create+end only)" $ whnfIO $ bareSpan activeTracer
           , bench "HTTP span (3 attrs)" $ whnfIO $ httpSpan activeTracer
           , bench "DB span (5 attrs)" $ whnfIO $ dbSpan activeTracer
           , bench "span + 2 events" $ whnfIO $ spanWithEvents activeTracer
           , bench "3-deep nested spans" $ whnfIO $ nestedSpans activeTracer
           , bench "heavy span (5 attrs + status + event)" $ whnfIO $ heavySpan activeTracer
           , bench "getSpanContext (live, isolated)" $ whnfIO $ do
               s <- createSpan activeTracer empty "s" defaultSpanArguments
               getSpanContext s
           ]
    , bgroup
        "rng"
        [ bench "SpanId (xoshiro)" $ whnfIO $ newSpanId DefaultIdGenerator
        , bench "TraceId (xoshiro)" $ whnfIO $ newTraceId DefaultIdGenerator
        , bench "SpanId+TraceId (3 separate)" $ whnfIO $ do
            !_ <- newTraceId DefaultIdGenerator
            newSpanId DefaultIdGenerator
        , bench "TraceId+SpanId (cmm primop)" $
            whnfIO $
              newTraceAndSpanId DefaultIdGenerator
        ]
    ]


mkKeys :: Int -> [T.Text]
mkKeys n = [T.pack ("k" <> show i) | i <- [1 .. n]]


-- | A map of @n@ distinct text attributes, @k1=v1 .. kN=vN@.
mkAttrMap :: Int -> A.AttributeMap
mkAttrMap = mkAttrMapFrom 1


-- | A map of @n@ distinct text attributes. The first key is @k<start>@.
mkAttrMapFrom :: Int -> Int -> A.AttributeMap
mkAttrMapFrom start n =
  H.fromList [(T.pack ("k" <> show i), A.toAttribute (T.pack ("v" <> show i))) | i <- [start .. start + n - 1]]


{- | The merge in this branch, reduced to the map so it compares like for
like with 'legacyMerge'.
-}
newMerge :: A.AttributeLimits -> A.Attributes -> A.AttributeMap -> A.AttributeMap
newMerge limits base attrs = A.getAttributeMap (A.addAttributeMap limits base attrs)


{- | The batch merge as it was before this branch: one 'H.insert' per key,
with a membership check against the base map, folded over the batch. It is
kept here so that the two versions run in the same benchmark binary. It
returns the merged map. The count and dropped fields of the original are
computed in the same fold, so the work is the same.
-}
legacyMerge :: A.AttributeLimits -> A.Attributes -> A.AttributeMap -> A.AttributeMap
legacyMerge A.AttributeLimits {..} base attrs =
  let attributeMap = A.getAttributeMap base
      attributesCount = A.getCount base
  in case attributeCountLimit of
       Nothing ->
         let (!newAttrs, !_added) =
               H.foldlWithKey'
                 (\(!m, !n) k v -> (H.insert k v m, if H.member k attributeMap then n else n + 1))
                 (attributeMap, 0 :: Int)
                 attrs
         in newAttrs
       Just limit_ ->
         let (!merged, !_accepted, !_totalNew) =
               H.foldlWithKey'
                 ( \(!m, !n, !seen) k v ->
                     if H.member k attributeMap
                       then (H.insert k v m, n, seen)
                       else
                         if n < limit_
                           then (H.insert k v m, n + 1, seen + 1)
                           else (m, n, seen + 1)
                 )
                 (attributeMap, attributesCount, 0 :: Int)
                 attrs
         in merged


mkAttrBuilder :: Int -> A.AttrsBuilder
mkAttrBuilder n = mconcat [A.attr (T.pack ("k" <> show i)) (T.pack ("v" <> show i)) | i <- [1 .. n]]


mkCountingProcessor :: IO SpanProcessor
mkCountingProcessor = do
  ref <- newAtomicCounter 0
  pure
    SpanProcessor
      { spanProcessorOnStart = \_ _ -> pure ()
      , spanProcessorOnEnd = \_ -> void $ incrAtomicCounter ref
      , spanProcessorShutdown = pure ShutdownSuccess
      , spanProcessorForceFlush = pure FlushSuccess
      }
