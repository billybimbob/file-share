#!/usr/bin/env python3

from __future__ import annotations

import asyncio
from io import BufferedReader
import pickle
import struct
import sys

from collections.abc import Awaitable
from enum import Enum
from typing import (
    Any, Callable, Generic, NamedTuple, Optional, TypeVar, Union, cast
)

from argparse import Namespace
from configparser import ConfigParser
from pathlib import Path


# Max amount read for files
CHUNK_SIZE = 1024


#region message passing

class Request(Enum):
    """ Request messages """
    DOWNLOAD = 'download'
    FILES = 'files_list'
    QUERY = 'query'
    UPDATE = 'update_files'

    @staticmethod
    def parse(poss_req: str) -> Request:
        poss_req = poss_req.strip().upper()
        request = Request[poss_req]

        return request


T = TypeVar('T')


class Procedure:
    """
    Request message wrapped with positional and keyword args.

    Each Request value has various types of expected arguments to be
    passed with. Below specifies the expects args, with the left side
    of the pipe being the initiating request, and the right side is
    the response type. The value after the :: indicates the expected
    type, N/A indicates that the request is not expected to be received,
    and None indicates that no args are expected

    Expected Arguments:
        DOWNLOAD: chunk :: File.Chunk
            | N/A
        FILES: None
            | files :: frozenset[File.Data]
        QUERY: file :: File.Data
            | response :: Response
        UPDATE: files :: frozenset[File.Data]
            | N/A
    """

    class Args:
        """ Combined view of all of the passed Procedure args """
        def __init__(self, args: tuple[Any, ...], kwargs: dict[str, Any]):
            self._names = kwargs
            self._pos = args

        def __getitem__(self, idx: int) -> Any:
            return self._pos[idx]

        def __getattribute__(self, name: str) -> Any:
            return self._names[name]


    _request: Request
    _args: tuple[Any, ...]
    _kwargs: dict[str, Any]

    def __init__(self, request: Request, *args: Any, **kwargs: Any) -> None:
        self._request = request
        self._args = args
        self._kwargs = kwargs

    @property
    def request(self):
        return self._request

    @property
    def args(self):
        return Procedure.Args(self._args, self._kwargs)

    def __call__(
        self,
        func: Callable[..., T],
        *args: Any,
        self_first: bool = False,
        **kwargs: Any) -> T:
        """
        Calls the passed function, supplying in a merge of all of the args.

        Arguments:
            func: function that is being called, with Procedure and passed args being supplied
            self_first: order of the positional and precedence of the keyword args

        Returns:
            Result of the passed function
        """
        pos_args = (
            args + self._args
            if not self_first else
            self._args + args
        )

        key_args = ( # 2nd keywords override
            self._kwargs | kwargs
            if not self_first else
            kwargs | self._kwargs
        )

        return func(*pos_args, **key_args)



class Message(Generic[T]):
    """ Information that is passed between socket streams """
    _payload: T # pre-pickled data

    HEADER = struct.Struct("!I")

    def __init__(self, payload: T):
        """
        Creates a message that can be packed; the payload should
        not be pickled yet
        """
        self._payload = payload


    @staticmethod
    async def get(stream: asyncio.StreamReader) -> Message[T]:
        """ Gets a message from the stream reader """
        header = await stream.readexactly(Message.HEADER.size)
        size, = Message.HEADER.unpack(header)

        data = await stream.readexactly(size)
        payload = pickle.loads(data)

        return Message[T](payload)


    def pack(self) -> bytes:
        """ Get the message with a metadata header for streams """
        data = pickle.dumps(self._payload)
        size = len(data)
        header = Message.HEADER.pack(size)
        return header + data


    def unwrap(self) -> T:
        """ Return the payload or raise it as a decoded exception """
        if isinstance(self._payload, Exception):
            raise self._payload
        else:
            return self._payload


    @staticmethod
    async def write(writer: asyncio.StreamWriter, payload: Any):
        """ Send a regular message or an exception through the stream """
        message = Message(payload)
        writer.write(message.pack())
        await writer.drain()


    @staticmethod
    async def read(reader: asyncio.StreamReader) -> T:
        """ Get and unwrap bytes, as the expected type from the stream """
        message = await Message[T].get(reader)
        return message.unwrap()


Receiver = Callable[[asyncio.StreamReader], Awaitable[T]]


class StreamPair(NamedTuple):
    """ Stream read-write pairing """
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def request(
        self,
        req_type: Request,
        *args: Any,
        as_type: Optional[type[T]] = None,
        receiver: Optional[Receiver[T]] = None,
        **kwargs: Any) -> T:
        """
        Sends a request with keyword arguments;
        Only one of as_type or receiver should be defined;
        Remaining args and kwargs are forwarded as Procedure args, see
        Procedure class for expected arg types

        Keyword arguments:
            as_type: specifies to use a default receiver where one message is read
            receiver: specifies how the stream will be read

        Returns:
            A response back if as_type or receiver is specified, otherwise
            returns None
        """
        procedure = Procedure(req_type, *args, **kwargs)
        await Message.write(self.writer, procedure)

        if receiver and as_type:
            raise ValueError("Only one of as_type or receiver can be defined")

        if receiver:
            return await receiver(self.reader)
        elif as_type:
            return await Message[as_type].read(self.reader)
        else:
            return cast(Any, None) # kind of hacky


class Location(NamedTuple):
    """ Main information for socket locations """
    host: str
    port: int


class Login(NamedTuple):
    """ Initial peer identification """
    id: str
    host: str
    port: int

    @property
    def location(self) -> Location:
        return Location(self.host, self.port)

#endregion


class File:
    """ Namespace for certain classes """

    class Data(NamedTuple):
        """ File name and it's total file size """
        name: str
        size: int

    class Chunk(NamedTuple):
        """ File request for download and upload """
        name: str
        offset: int
        size: int


class Response(NamedTuple):
    """ Query response from the indexer node """
    file: File.Data
    locations: frozenset[Location]



def getpeerbystream(
    stream: Union[StreamPair, asyncio.StreamWriter]) -> tuple[str, int]:
    """ Adds type checks to get_extra_info method for Transport """
    if isinstance(stream, StreamPair):
        stream = stream.writer

    poss_info = stream.get_extra_info('peername')

    if (poss_info is None
        or len(poss_info) < 2
        or not isinstance(poss_info[0], str)
        or not isinstance(poss_info[1], int)):
        raise RuntimeError('cannot get peer information')

    return poss_info[:2]


def chunk_iter(file_ptr: BufferedReader, read_limit: int):
    """ Reads the reader in chunks, to help avoid large memory loads """
    amt_yield = 0
    while amt_yield < read_limit:
        chunk = file_ptr.read(CHUNK_SIZE)
        limit = min(len(chunk), read_limit-amt_yield)
        chunk = chunk[:limit]
        amt_yield += len(chunk)

        yield chunk


async def ainput(prompt: str='') -> str:
    """ Async version of user input """
    return await asyncio.to_thread(input, prompt)


def as_obj(src: dict[str, Any]) -> object:
    """
    Convert a dictionary into a python object, where each key is an attribute
    """
    obj = object()
    obj.__dict__.update(src)
    return obj


def read_main_config(filename: str) -> dict[str, Union[str, int]]:
    """ Parse an ini file into a dictionary, ignoring sections """
    if not Path(filename).exists():
        raise IOError("Config file does not exist")

    conf = ConfigParser()
    conf.read(filename)

    args = dict[str, Union[str, int]]()

    main = conf['main']
    for arg in main:
        val = main[arg]
        arg = arg.replace('-', '_')
        args[arg] = int(val) if val.isnumeric() else val

    return args


def merge_config_args(args: Namespace) -> dict[str, Any]:
    """ If config file specified, merge in config arguments as a dictionary """
    if not args.config:
        return vars(args)
    else:
        # config overrides command args
        merged_args = vars(args) | read_main_config(args.config)
        return merged_args


def version_check():
    if sys.version_info < (3,9):
        raise RuntimeError("Python version needs to be at least 3.9")
