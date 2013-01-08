{-# LANGUAGE OverloadedStrings #-}

module RedisParser (
	multi_bulk_parser, server_parser, server_parser_multi, Reply(..),
	cmd2stream, arg2stream
) where


import Data.Int (Int64)

import           Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BSL



showInt :: Int64 -> ByteString
showInt a = BSL.pack $ show a


multi_bulk_parser :: ByteString -> Maybe (ByteString, Maybe [Maybe ByteString])
multi_bulk_parser s = do
	(s, c)  <- get_bulk_size s '*'
	get_args s c []
	where
		get_args :: ByteString -> Int64 -> [Maybe ByteString] -> Maybe (ByteString, Maybe [Maybe ByteString])
		get_args s (-1) as = return (s, Nothing)
		get_args s 0    as = return (s, Just $ reverse as)
		get_args s c    as = do
			(s, a) <- get_bulk_arg s
			get_args s (c - 1) (a:as)


get_bulk_arg :: ByteString -> Maybe (ByteString, Maybe ByteString)
get_bulk_arg s = do
	(s, c) <- get_bulk_size s '$'
	get_bulk_value s c


get_bulk_size :: ByteString -> Char -> Maybe (ByteString, Int64)
get_bulk_size s char = do
	(f, s) <- BSL.uncons s
	when_ (f == char)
	(s, c) <- readInt64CRLF s
	return (s, c)


get_bulk_value :: ByteString -> Int64 -> Maybe (ByteString, Maybe ByteString)
get_bulk_value s (-1) = return (s, Nothing)
get_bulk_value s c    = do
	let a = BSL.take c s
	when_ (isCRLF s c)
	let t = BSL.drop (c + 2) s
	return (t, Just a)


isCRLF :: ByteString -> Int64 -> Bool
isCRLF s i = BSL.index s i == '\r' && BSL.index s (i + 1) == '\n'

when :: Bool -> a -> Maybe a
when False _ = Nothing
when True  a = Just a

when_ :: Bool -> Maybe Bool
when_ a = when a True


readInt64CRLF :: ByteString -> Maybe (ByteString, Int64)
readInt64CRLF s = do
	(c, s) <- BSL.readInteger s
	when_ (isCRLF s 0)
	let t = BSL.drop 2 s
	return (t, fromIntegral c)


data Reply = RInt Int64 | RInline ByteString | RBulk (Maybe ByteString) | RMultiSize Int64

server_parser :: ByteString -> Maybe (ByteString, Reply)
server_parser s = do
	(h, _) <- BSL.uncons s
	case h of
		'+' -> do
			(s, c) <- beforeCRLF s
			return (s, RInline c)
		'-' -> do
			(s, c) <- beforeCRLF s
			return (s, RInline c)
		':' -> do
			(s, c) <- readInt64CRLF $ BSL.drop 1 s
			return (s, RInt c)
		'$' -> do
			(s, c) <- get_bulk_size s '$'
			(s, a) <- get_bulk_value s c
			return (s, RBulk a)
		'*' -> do
			(s, c) <- readInt64CRLF $ BSL.drop 1 s
			return (s, RMultiSize c)


server_parser_multi :: ByteString -> Maybe (ByteString, Reply)
server_parser_multi s = do
	(s, c) <- get_bulk_size s '$'
	(s, a) <- get_bulk_value s c
	return (s, RBulk a)


beforeCRLF :: ByteString -> Maybe (ByteString, ByteString)
beforeCRLF s = do
	ri <- BSL.elemIndex '\r' s
	case BSL.index s (ri + 1) == '\n' of
		True  -> do
			let h = BSL.take (ri) s
			let t = BSL.drop (ri + 2) s
			return (t, h)
		False -> do
			let h = BSL.take (ri + 1) s
			let t = BSL.drop (ri + 1) s
			(t2, h2) <- beforeCRLF t
			return (t2, BSL.concat [h, h2])



-- Преобразование команды (список аргументов) в строку, поток байтов, соответствующий протоколу redis.
cmd2stream :: [Maybe ByteString] -> [ByteString]
cmd2stream [] = ["*0\r\n"]
cmd2stream as = ["*", (showInt $ fromIntegral $ length as), "\r\n"] ++ concat (map arg2stream as)


-- Преобразование аргумента в строку, поток байтов, соответствующий протоколу redis.
arg2stream :: Maybe ByteString -> [ByteString]
arg2stream Nothing  = ["$-1\r\n"]
arg2stream (Just s) = ["$", (showInt $ fromIntegral $ BSL.length s), "\r\n", s, "\r\n"]
