{-# LANGUAGE OverloadedStrings #-} -- this overloads String literals default to ByteString
 
module Network.Wai.Middleware.Braid.Internal
    (
        -- * Types
        Update, Topic,
        -- * Method helpers
        isGetRequest, isPutRequest, isPatchRequest,
        -- * 209 Status variable
        status209,
        -- * Header helpers & variables
        hSub, hVer, hParents, hPatch,
        lookupHeader,
        getSubscription, hasSubscription, getSubscriptionKeepAliveTime, addSubscriptionHeader,
        getVersion, hasVersion, addVersionHeader,
        getParents, hasParents,
        getPatches, hasPatches,
        -- * Update helpers
        requestToUpdate, updateToBuilder

    ) where

import Network.Wai                                      (Request(..), Response(..), requestMethod, requestHeaders, mapResponseHeaders)
import Network.HTTP.Types.Method                        (Method, methodGet, methodPut, methodPatch)
import Network.HTTP.Types.Header                        (Header, HeaderName, RequestHeaders, ResponseHeaders)
import Network.HTTP.Types.Status                        (Status, mkStatus)
import Network.Wai.Middleware.AddHeaders                (addHeaders)
import qualified Data.ByteString            as B        (ByteString, append, intercalate)
import qualified Data.ByteString.Lazy       as L        (ByteString, toStrict)
import qualified Data.CaseInsensitive       as CI       (CI, original)
import qualified Data.ByteString.Char8      as BC       (breakSubstring, pack)
import Data.ByteString.Builder                          (Builder, byteString, string7)
import Data.Text                                        (Text)
import Data.Maybe                                       (isJust)


--------------------------------------------------------------------------------
-- TYPES --

type Topic = [Text]

data Update 
    -- | Updates are streamed from the server to subcribing client.
    -- On a PUT request, the headers and request body are put into an Update and streamed to subscribing clients.
    = Update {
    -- | The updateTopic is formed, from the request path
    updateTopic :: [Text],
    -- | The updateHeader are taken straight from the request headers
    updateHeaders :: RequestHeaders,
    -- | The updatePatches correspond to the request body
    updatePatches :: L.ByteString
}

--------------------------------------------------------------------------------
-- METHODS --

isGetRequest, isPutRequest, isPatchRequest :: Request -> Bool
isGetRequest req = requestMethod req == methodGet
isPutRequest req = requestMethod req == methodPut
isPatchRequest req = requestMethod req == methodPatch

--------------------------------------------------------------------------------
-- STATUS --

-- | 209 Subscription is the new status code for subscriptions in braid
status209 :: Status
status209 = mkStatus 209 "Subscription"

--------------------------------------------------------------------------------
-- HEADERS --

lookupHeader :: HeaderName -> [Header] -> Maybe B.ByteString
lookupHeader _ [] = Nothing
lookupHeader v ((n, s):t)  
    |  v == n   =  Just s
    | otherwise =  lookupHeader v t

-- SUBSCRIPTION HEADER HELPERS --

hSub :: HeaderName
hSub = "Subscribe"

getSubscription :: Request -> Maybe B.ByteString
getSubscription req = lookupHeader hSub $ requestHeaders req

getSubscriptionKeepAliveTime :: Request -> B.ByteString
getSubscriptionKeepAliveTime req =
    let Just str = lookupHeader hSub $ requestHeaders req 
    in snd $ BC.breakSubstring "=" str

hasSubscription :: Request -> Bool
hasSubscription req = isJust $ getSubscription req

addSubscriptionHeader :: B.ByteString -> Response -> Response
addSubscriptionHeader s = mapResponseHeaders (\hs -> (hSub, s) : ("Cache-Control", "no-cache, no-transform") : hs)

-- VERSION HEADER HELPERS --

hVer :: HeaderName   
hVer = "Version"

getVersion :: Request -> Maybe B.ByteString
getVersion req = lookupHeader hVer $ requestHeaders req

hasVersion :: Request -> Bool
hasVersion req = isJust $ getVersion req

addVersionHeader :: B.ByteString -> Response -> Response
addVersionHeader s = mapResponseHeaders (\hs -> (hVer, s) : hs)

-- PARENTS HEADER HELPERS --

hParents :: HeaderName
hParents = "Parents"

getParents :: Request -> Maybe B.ByteString
getParents req = lookupHeader hParents $ requestHeaders req

hasParents :: Request -> Bool
hasParents req = isJust $ getParents req

-- PATCHES HEADER HELPERS --

hPatch :: HeaderName
hPatch = "Patches"

getPatches :: Request -> Maybe B.ByteString
getPatches req = lookupHeader hPatch $ requestHeaders req

hasPatches :: Request -> Bool
hasPatches req = isJust $ getPatches req

--------------------------------------------------------------------------------
-- UPDATES --

-- | Forms an Update from a WAI Request
requestToUpdate :: Request -> L.ByteString -> Update
requestToUpdate req body = Update {
    updateTopic = pathInfo req,
    updateHeaders = requestHeaders req,
    updatePatches = body
}


separator :: B.ByteString
separator = BC.pack ": "

-- | Turns an Update (headers and patches) into a Builder to be streamed
-- Will return Nothing if the Topic we pass doesn't not match the updateTopic in the Update
-- Or returns Just builder, where builder has type Builder
updateToBuilder :: Topic -> Update -> Maybe Builder
updateToBuilder topic (Update t h p) 
    | t /= topic = Nothing
    | otherwise = Just builder
    where
        {-| 
            Forming:
                HEADERS \n
                \n
                BODY
            Then converting the ByteString to a Builder.
            Here's an example:
                Version: "ej4lhb9z78"                                 | HEADERS
                Parents: "oakwn5b8qh", "uc9zwhw7mf"                   |
                Content-Type: application/json                        |
                Merge-Type: sync9                                     |
                Content-Length: 64                                    |
                                                                      |
                [{"text": "Hi, everyone!",                            | | BODY
                "author": {"link": "/user/tommy"}}]                   | |
        -}
        updateToBuilder' :: RequestHeaders -> L.ByteString -> Builder
        updateToBuilder' hs b =
            byteString $ headers `B.append` "\n\n" `B.append` L.toStrict b
            where
                {-| 
                    Essentially, we are turning [(Header1, Value1), ..., (HeaderN, ValueN)]
                    to  "
                            Header1: Value1 \n
                            .
                            .
                            .
                            HeaderN: ValueN
                        "
                -}
                headers = B.intercalate "\n" $ map (\(h, v) -> CI.original h `B.append` separator `B.append` v) hs
        builder = updateToBuilder' h p