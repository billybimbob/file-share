#!/usr/bin/env python3

from __future__ import annotations

from typing import (
    Any, Callable, Generic, NamedTuple, Optional, TypeVar, Union, cast
)
from dataclasses import dataclass
from collections.abc import Awaitable
from enum import Enum

from pathlib import Path
from configparser import ConfigParser
from argparse import Namespace

import sys
import struct
import pickle
import asyncio


# Max amount read for files
CHUNK_SIZE = 1024


#region message passing

class Request(Enum):
    """ Request messages """
    FILES = 'files_list'
    DOWNLOAD = 'download'
    UPDATE = 'update_files'
    QUERY = 'query'
    HIT = 'hit'

    @staticmethod
    def parse(poss_req: str) -> Request:
        poss_req = poss_req.strip().upper()
        request = Request[poss_req]

        return request


T = TypeVar('T')


@dataclass(frozen=True)
class Procedure:
    """ Request message wrapped with positional and keyword args """
    request: Request
    _args: tuple[Any, ...]
    _kwargs: dict[str, Any]

    def __call__(
        self,
        func: Callable[..., T],
        *args: Any,
        self_first: bool = False,
        **kwargs: Any) -> T:
        """
        Calls the passed function, supplying in a merge of all of the args.
        The order of the positional and precedence of the keyword args is
        based on self_first
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
        Sends a request with keyword arguments, and gets a response back if as_type 
        or receiver is specified, otherwise returns None. Only one of as_type or 
        receiver should be defined; receiver specifies how the stream will be read 
        in order to generate a value, while as_type specifies to use a default 
        receiver where one message is read, and converted to the type
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


class Login(NamedTuple):
    """ Initial peer identification """
    user: str
    host: str
    port: int

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
    location: frozenset[tuple[str, int]]

#endregion


def getpeerbystream(stream: Union[StreamPair, asyncio.StreamWriter]) -> Optional[tuple[str, int]]:
    """ Adds type checks to get_extra_info method for Transport """
    if isinstance(stream, StreamPair):
        stream = stream.writer

    poss_info = stream.get_extra_info('peername')

    if (poss_info is not None
        and len(poss_info) >= 2
        and isinstance(poss_info[0], str)
        and isinstance(poss_info[1], int)):
        return poss_info[:2]
    else:
        return None


async def ainput(prompt: str='') -> str:
    """ Async version of user input """
    return await asyncio.to_thread(input, prompt)


# def shallow_obj(map: Dict[str, Any]) -> object:
#     """
#     Convert a dictionary into a python object, where each key is an attribute
#     """
#     return namedtuple('obj', map.keys())(*map.values())


def read_config(filename: str) -> dict[str, Any]:
    """ Parse an ini file into a dictionary, ignoring sections """
    if not Path(filename).exists():
        raise IOError("Config file does not exist")

    conf = ConfigParser()
    conf.read(filename)

    args: dict[str, Any] = {}
    for section in conf.sections():
        for arg in conf[section]:
            args[arg] = conf[section][arg]
            if args[arg].isnumeric():
                args[arg] = int(args[arg])

    return args


def merge_config_args(args: Namespace) -> dict[str, Any]:
    """ If config file specified, merge in config arguments as a dictionary """
    if not args.config:
        return vars(args)
    else:
        # config overrides command args
        merged_args = vars(args) | read_config(args.config)
        return merged_args


def version_check():
    if sys.version_info < (3,9):
        raise RuntimeError("Python version needs to be at least 3.9")
