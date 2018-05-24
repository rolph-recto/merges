{-# LANGUAGE DeriveFunctor #-}

-- CRDT.hs
-- a model of CRDT supported by algebraic effects

import Control.Monad.State
import Control.Monad.Writer
import Control.Monad.Except
import Control.Monad.Free
import qualified Data.Map.Strict as M

data HandlerArg =
    HandlerVoid
  | HandlerInt Int
  | HandlerBool Bool
  | HandlerString String

instance Show HandlerArg where
  show a = case a of
    HandlerVoid -> "()"
    HandlerInt n -> show n
    HandlerBool b -> show b
    HandlerString s -> s

type CRDTProgram a  = Free CRDTCmd a
type CRDTCont       = HandlerArg -> CRDTProgram ()
type CRDTHandler    = CRDTCont -> HandlerArg -> CRDTProgram ()
type CRDTHandlerMap = M.Map String CRDTHandler

-- behavior of a regHandle command when handlers conflict
data OverlapBehavior =
    DeferHandler -- use the existing handler instead of the new one
  | OverrideHandler -- override the existing handler 

data CRDTCmd next = 
    CRDTTry CRDTHandlerMap (CRDTProgram ()) next 
  | CRDTRaise String HandlerArg (HandlerArg -> next)
  | CRDTRegHandler OverlapBehavior String CRDTHandler next
  | CRDTPrint String next
  deriving (Functor)

liftFree x    = Free (fmap Pure x)

-- notes:
-- it looks like we want someting like dynamically scoped variables
-- for registering handlers. but:
-- * maybe we want to restrict the scope somehow (e.g. datatypes in one
--   context have a handler to datastore 1, datatypes in another context have
--   a handler to datastore 2)
--
-- * maybe we just want the reader monad, where the extra context contains
--   the handler; functions oblivious to the extra context can just be lifted
--   to the monad

-- try registers a handler that only works in the scope of its block
try cmd             = liftFree (CRDTTry M.empty cmd ())
tryWithHandle h cmd = liftFree (CRDTTry h cmd ())

-- regHandler registers a handler for the rest of the program;
-- it is completely unscoped!
--
-- the default behavior of regHandler is to defer to the
-- existing handler in case of overlaps; this assumes 
regHandler name h   = liftFree (CRDTRegHandler DeferHandler name h ())

raise e arg         = liftFree (CRDTRaise e arg id)
crdtPrint s         = liftFree (CRDTPrint s ())

data CRDTRuntime = CRDTRuntime {
  handlers :: M.Map String CRDTHandler 
}

type CRDTInterp a = ExceptT String (StateT CRDTRuntime IO) a

crdtInterp :: CRDTProgram () -> CRDTInterp ()
crdtInterp prog = case prog of
  Free (CRDTTry h tryProg next) -> do
    st <- get
    let curHMap = handlers st

    -- if a handler already exists, prefer that one instead of the
    -- new handler in h; parent handlers dominate child handlers
    let newHmap = M.union curHMap h
    put $ st { handlers = newHmap }
    crdtInterp tryProg

    -- restore old handler map
    put st
    crdtInterp next

  Free (CRDTRegHandler overlap name h next) -> do
    st <- get
    let hmap = handlers st
    let hmap' =
          case M.lookup name hmap of
            Nothing -> M.insert name h hmap
            Just _ -> case overlap of
              DeferHandler -> hmap
              OverrideHandler -> M.insert name h hmap

    put $ st { handlers = hmap' }
    crdtInterp next

  Free (CRDTRaise e arg next) -> do
    st <- get
    let hmap = handlers st 
    case M.lookup e hmap of
      Just h -> do
        crdtInterp (h next arg)

      Nothing -> throwError $ "Handler for effect " ++ e ++ " not found!"

  Free (CRDTPrint s next) -> do
    liftIO $ putStrLn s
    crdtInterp next

  Pure _ -> return ()

handlerResume :: CRDTCont -> HandlerArg -> CRDTProgram ()
handlerResume k arg = k arg

handlerBreak :: CRDTProgram ()
handlerBreak = return ()

crdtRun :: CRDTProgram () -> IO ()
crdtRun main = do
  let init = CRDTRuntime { handlers = M.empty }
  res <- evalStateT (runExceptT (crdtInterp main)) init
  case res of
    Left err  -> putStrLn err
    Right _   -> return ()

-- example 1: after division by zero, do not return
divHandler :: CRDTHandler
divHandler _ _ = do
  crdtPrint "division by zero error!"
  handlerBreak

crdtMain :: CRDTProgram ()
crdtMain = do
  let hmap = M.fromList [("divError", divHandler)]
    
  tryWithHandle hmap $ do
    crdtPrint "Nice!"
    arg <- raise "divError" HandlerVoid
    crdtPrint "this is the rest of the function. you should not see this."
    crdtPrint $ show arg

  crdtPrint "You should see this, tho"
  

-- example 2: the first update handler registered by the parent should be used,
-- regardless of other child handlers
updateHandler :: String -> CRDTHandler
updateHandler hname k arg = do
  crdtPrint $ "update handled by " ++ hname
  k HandlerVoid

{--
addSet :: 


crdtMain2 :: CRDTProgram ()
--}


