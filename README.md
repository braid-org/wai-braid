# wai-braid
braid protocol server in haskell, implemented as wai middleware

[my writeup](https://github.com/ghiliweld/writings/blob/master/braid.md) on the [braid protocol](https://braid.news/)

### Usage

```hs
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Network.Wai.Middleware.Braid (braidify, streamUpdates, hasSubscription, status209, Update)
import Network.Wai
import Control.Concurrent.Chan (Chan, newChan)
import Network.Wai.Handler.Warp (run)
import Network.HTTP.Types.Status (status200)
    
application src req respond = respond $ 
    if hasSubscription req 
    then responseStream status209 [("Content-Type", "text/plain")] $ streamUpdates src ["topic"] (Nothing)
    else responseLBS status200 [("Content-Type", "text/plain")] "Hello World"

{-| 
    instead of Nothing we can have (Just client), where client = lookupHeader "Client" $ requestHeaders req, the client field is for preventing echo updates by clients subscribing and sending updates -}

main :: IO ()
main = newChan >>= \src -> run 3000 $ braidify src $ application src
```

### example tests in cURL
![curl output](https://pbs.twimg.com/media/EqXRGRtWMAAaYCE?format=jpg&name=4096x4096)
