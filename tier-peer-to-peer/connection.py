#!/usr/bin/env python3

from __future__ import annotations

import asyncio
import pickle
import struct
import sys

from collections.abc import Awaitable
from dataclasses import dataclass
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
    HIT = 'hit'
    QUERY = 'query'
    UPDATE = 'update_files'

    @staticmethod
    def parse(poss_req: str) -> Request:
        poss_req = poss_req.strip().upper()
        request = Request[poss_req]

        return request


T = TypeVar('T')


@dataclass(frozen=True)
class Procedure:
    """
    Request message wrapped with positional and keyword args.

    Each Request value has various types of expected arguments to be
    passed with.

    Expected arguments:
        DOWNLOAD -- filename: str
        FILES -- None
        HIT -- hit: QueryHit
        QUERY -- query: Query | filename: str
        UPDATE -- files: frozenset[str]
    """
    request: Request
    _args: tuple[Any, ...]
    _kwargs: dict[str, Any]

    class Args:
        def __init__(self, args: tuple[Any, ...], kwargs: dict[str, Any]):
            self._names = kwargs
            self._pos = args

        def __getitem__(self, idx: int) -> Any:
            return self._pos[idx]

        def __getattribute__(self, name: str) -> Any:
            return self._names[name]

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
            func: function that is being called, will Procedure and passed args being supplied
            self_first: order of the positional and precedence of the keyword args

        Returns:
            Result of the pass function
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
            # potentially could have message of just random bytes
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
        procedure = Procedure(req_type, args, kwargs)
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
    is_super: bool = False

    @property
    def location(self) -> Location:
        return Location(self.host, self.port)

#endregion


#region query messaging across super peers

class Query(NamedTuple):
    """
    Information broadcasted to super peers for a file location
    """
    id: str
    filename: str
    alive_time: float

    def elapsed(self, time_change: float) -> Query:
        """ Creates an updated query, with the alive_time updated """
        return Query(self.id, self.filename, self.alive_time - time_change)


class QueryHit(NamedTuple):
    """ Response to a location query """
    id: str
    filename: str
    locations: set[Location]

#endregion


def getpeerbystream(stream: Union[StreamPair, asyncio.StreamWriter]) -> tuple[str, int]:
    """ Adds type checks to get_extra_info method for Transport """
    if isinstance(stream, StreamPair):
        stream = stream.writer

    poss_info = stream.get_extra_info('peername')
    return poss_info[:2]


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
