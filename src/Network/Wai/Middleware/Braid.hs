{-# LANGUAGE OverloadedStrings #-} -- this overloads String literals default to ByteString
-- |
-- Module      :  Network.Wai.Middleware.Braid
-- Copyright   :  © 2020–present Ghilia Weldesselasie
-- License     :  BSD 3 clause
--
-- Maintainer  :  Ghilia Weldesselasie <ghiliaweld@gmail.com>
-- Stability   :  experimental
-- Portability :  portable
--
-- === About the library
--
-- braidify
-- ---------
-- braidify acts as a wai middleware, it :
-- 1. adds ('Range-Request-Allow-Methods', 'PATCH, PUT'), ('Range-Request-Allow-Units', 'json'), ("Patches", "OK") headers to response
-- 2. adds Patches OK header to response if the request is a PUT request
-- 4. adds Version header to the response if request has a Version header and is a GET request
-- 5. applies the subscription middleware, which:
--     * modifies the status to 209 Subscription (if request has a Subscription header)
--     * adds Subscribe header to response (if request has a Subscription header)
--     * catches updates and writes them to a Update channel to be propagated to subscribers (if request is a PUT request)
--
-- all of this is in line with the braid protocol spec, outlined here:
--     https://raw.githubusercontent.com/braid-work/braid-spec/master/draft-toomim-httpbis-braid-http-03.txt
--
-- Subscriptions
-- -------------
-- when we get a subscription request we set up a watcher for updates at a certain channel,
-- we then wait for updates to that channel to stream them to the client
--
-- we define 2 helper functions for managing subscriptions:
--  * catchUpdate
--  * streamUpdates
--
-- catchUpdate
-- ------------
-- this will be setup on each PUT request to catch these updates, make a patch from it and funnel them to the appropriate channel.
-- Implemented as middleware and included in subscriptionMiddleware.
--
-- streamUpdates
-- ------------
-- takes a channel and a topic, and watches for new updates to that channel, return a StreamingBody that writes those updates to the client.
-- on any GET route that can be subcribed to, use streamUpdates if the request is a subscription request.

module Network.Wai.Middleware.Braid
    ( 
        -- * Middleware
        braidify,
        subscriptionMiddleware,
        versionMiddleware,
        addPatchHeader,
        -- * Subscription helper
        streamUpdates,
        -- * Types
        Update, Topic,
        -- * Method helpers
        isGetRequest, isPutRequest, isPatchRequest,
        -- * 209 Status variable
        status209,
        -- * Header helpers & variables
        hSub, hVer, hMerge, hParents, hPatch,
        lookupHeader,
        getSubscription, hasSubscription, getSubscriptionKeepAliveTime, addSubscriptionHeader,
        getVersion, hasVersion, addVersionHeader,
        getMergeType, hasMergeType, addMergeTypeHeader,
        getParents, hasParents,
        getPatches, hasPatches

    ) where

import Network.Wai          (Response(..), Request(..), Middleware, StreamingBody, responseStream, modifyResponse, ifRequest, mapResponseHeaders, mapResponseStatus, strictRequestBody)
import Network.HTTP.Types.Header (Header, HeaderName, RequestHeaders, ResponseHeaders)
import Network.Wai.Middleware.AddHeaders (addHeaders)
import Control.Concurrent.Chan (Chan, dupChan, readChan, writeChan)
import Control.Monad.IO.Class (liftIO)
import Data.Function (fix)
import Data.ByteString (ByteString)
import Data.ByteString.Builder                          (Builder, byteString, string7)

import Network.Wai.Middleware.Braid.Internal
    ( isGetRequest,
      isPutRequest,
      isPatchRequest,
      Update,
      Topic,
      status209,
      requestToUpdate,
      updateToBuilder,
      lookupHeader,
      hSub,
      getSubscription,
      getSubscriptionKeepAliveTime,
      hasSubscription,
      addSubscriptionHeader,
      hVer,
      getVersion,
      hasVersion,
      addVersionHeader,
      hMerge,
      getMergeType,
      hasMergeType,
      addMergeTypeHeader,
      hParents,
      getParents,
      hasParents,
      hPatch,
      getPatches,
      hasPatches )


-- TODO: still needs mechanism to keep alive, i.e. keeping the response connection open
subscriptionMiddleware :: Chan Update -> Middleware
subscriptionMiddleware src = catchUpdate src . modifyHeadersToSub . modifyStatusTo209
    where
        modifyHeadersToSub :: Middleware
        modifyHeadersToSub app req respond = 
            case getSubscription req of
                Just v -> app req $ respond . addSubscriptionHeader v
                Nothing -> app req respond
        modifyStatusTo209 :: Middleware
        modifyStatusTo209 = ifRequest hasSubscription $ modifyResponse $ mapResponseStatus (const status209)
        -- NOTE: we're consuming the full request body, maybe there's a better way of doing this? idk
        catchUpdate :: Chan Update -> Middleware
        catchUpdate src = ifRequest isPutRequest 
            $ \app req res -> do
                src' <- liftIO $ dupChan src
                strictRequestBody req >>= \b ->  
                    writeChan src' $ requestToUpdate req b 
                >> app req res

versionMiddleware :: Middleware
versionMiddleware app req respond = 
    case (getVersion req, isGetRequest req) of
        (Just v, True) -> app req $ respond . addVersionHeader v
        _              -> app req respond

addPatchHeader :: Middleware
addPatchHeader = ifRequest isPutRequest $ addHeaders [("Patches", "OK")]

{-| 
    TODO: look into Chan vs BroadcastChan (https://github.com/merijn/broadcast-chan)
-}

streamUpdates :: Chan Update -> Topic -> Maybe ByteString -> StreamingBody
streamUpdates chan topic client write flush = do
        flush
        src <- liftIO $ dupChan chan
        fix $ \loop -> do
            update <- readChan src
            case updateToBuilder topic client update of
                Just b -> write b >> flush >> loop
                Nothing -> loop

braidify :: Chan Update -> Middleware
braidify src =
    subscriptionMiddleware src
    . versionMiddleware
    . addPatchHeader
    . addHeaders [("Range-Request-Allow-Methods", "PATCH, PUT"), ("Range-Request-Allow-Units", "json")]