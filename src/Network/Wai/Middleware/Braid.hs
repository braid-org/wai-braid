{-# LANGUAGE OverloadedStrings #-} -- this overloads String literals default to ByteString

module Network.Wai.Middleware.Braid
    ( 
        -- * Middleware
        braidify
    ) where

import Network.HTTP.Types.Method   (Method, methodGet, methodPut, methodPatch)
import Network.HTTP.Types.Header   (Header, HeaderName, RequestHeaders, ResponseHeaders)
import Network.HTTP.Types.Status   (Status, mkStatus)
import Network.Wai          (Middleware, responseStream, modifyResponse, ifRequest, mapResponseHeaders, mapResponseStatus)
import Network.Wai.Middleware.AddHeaders (addHeaders)
import Network.Wai.EventSource (ServerEvent, eventData)
import Network.Wai.Internal (Response(..), Request(..))
import Data.ByteString      (ByteString, breakSubstring)
import qualified Data.CaseInsensitive  as CI
import qualified Data.ByteString.Char8 as BC
import Data.Function (fix)
import Data.Foldable (for_)
import Control.Concurrent.Chan (Chan, dupChan, readChan)

isGetRequest, isPutRequest, isPatchRequest :: Request -> Bool
isGetRequest req = requestMethod req == methodGet
isPutRequest req = requestMethod req == methodPut
isPatchRequest req = requestMethod req == methodPatch

-- new status code for subscriptions in braid
status209 :: Status
status209 = mkStatus 209 "Subscription"

modifyStatusTo209 :: Middleware
modifyStatusTo209 = modifyResponse $ mapResponseStatus (const status209)

lookupHeader :: HeaderName -> [Header] -> Maybe ByteString
lookupHeader _ [] = Nothing
lookupHeader v ((n, s):t)  
    |  v == n   =  Just s
    | otherwise =  lookupHeader v t

hSub :: HeaderName
hSub = "Subscribe"

getSubscription :: Request -> Maybe ByteString
getSubscription req = lookupHeader hSub $ requestHeaders req

getSubscriptionKeepAliveTime :: Request -> ByteString
getSubscriptionKeepAliveTime req =
    let Just str = lookupHeader hSub $ requestHeaders req 
    in snd $ breakSubstring "=" str

hasSubscription :: Request -> Bool
hasSubscription req = 
    case getSubscription req of
        Just s -> True
        Nothing -> False

-- still have to add ('Cache-Control', 'no-cache, no-transform') and ('content-type', 'application/json') to headers
addSubscriptionHeader :: ByteString -> Response -> Response
addSubscriptionHeader s = mapResponseHeaders (\hs -> (hSub, s) : ("Cache-Control", "no-cache, no-transform") : hs)

-- still needs mechanism to keep alive, i.e. keeping the response connection open
subscriptionMiddleware :: Middleware
subscriptionMiddleware = subscriptionMiddleware' . modifyStatusTo209
    where
        subscriptionMiddleware' :: Middleware
        subscriptionMiddleware' app req respond = 
            case getSubscription req of
                Just v -> app req $ respond . addSubscriptionHeader v
                Nothing -> app req respond

hVer :: HeaderName   
hVer = "Version"

getVersion :: Request -> Maybe ByteString
getVersion req = lookupHeader hVer $ requestHeaders req

hasVersion :: Request -> Bool
hasVersion req =
    case getVersion req of
        Just s -> True
        Nothing -> False

addVersionHeader :: ByteString -> Response -> Response
addVersionHeader s = mapResponseHeaders (\hs -> (hVer, s) : hs)

versionMiddleware :: Middleware
versionMiddleware app req respond = 
    case (getVersion req, isGetRequest req) of
        (Just v, True) -> app req $ respond . addVersionHeader v
        _              -> app req respond

addMergeTypeHeader :: Middleware
addMergeTypeHeader = ifRequest isGetRequest $ addHeaders [("Merge-Type", "sync9")]

addPatchHeader :: Middleware
addPatchHeader = ifRequest isPutRequest $ addHeaders [("Patches", "OK")]

hPatch :: HeaderName
hPatch = "Patches"

getPatches :: Request -> Maybe ByteString
getPatches req = lookupHeader hPatch $ requestHeaders req

hasPatches :: Request -> Bool
hasPatches req = 
    case getPatches req of
        Just s -> True
        Nothing -> False

{-|
    braidify
    ---------
    braidify acts as a wai middleware, it :
    1. sets header to ('Range-Request-Allow-Methods', 'PATCH, PUT'), ('Range-Request-Allow-Units', 'json'), ("Patches", "OK")
    2. parse header for version, parents, and subscription
    3. add sendVersion, patches(JSON), and startSubscription helpers to request and response
-}

braidify :: Middleware
braidify =
    versionMiddleware
    . subscriptionMiddleware
    . addMergeTypeHeader
    . addPatchHeader
    . addHeaders [("Range-Request-Allow-Methods", "PATCH, PUT"), ("Range-Request-Allow-Units", "json")]

{-|
    Subscriptions
    -------------
    when we get a subscription request we set up a watcher for updates at a certain channel,
    we then wait for updates to that channel to send them to the client

    we define 3 helper functions for managing subscriptions:
    - catchUpdate
    - makePatch
    - sendPatch

    note: 
        - write a robust patch type Patch
        - write a newPatchChan function to instantiate a (Chan Patch) channel
        - write a urlPatchMap to map raw url paths to Patch channels

    --REQUEST SIDE--

    catchUpdate
    ------------
    this will be setup on each PUT request to catch these updates, make a patch from it and funnel them to the appropriate channel,
    maybe implement as middleware?

    note: uses makePatch

    catchUpdate :: Middleware
    catchUpdate = ifRequest isPutRequest $ funnelUpdate makePatch urlPathMaps

    makePatch
    ------------
    this takes a PUT request, parse it, and makes a version update out of it.

    --RESPONSE SIDE---
    on any GET route that can be subcribed to, add sendPatch if the request is a subscription request
    
    sendPatch
    ------------
    takes in a channel and watches for new updates to that channel, writing those updates to the client.

-}