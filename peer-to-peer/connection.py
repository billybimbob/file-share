#!/usr/bin/env python3

from __future__ import annotations
from typing import Any, Awaitable, Callable, NamedTuple, Optional, TypeVar, Union, cast
from enum import Enum
from io import UnsupportedOperation
# from collections import namedtuple

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
    GET_FILES = 'get_files_list'
    DOWNLOAD = 'download'
    UPDATE = 'update_files'
    QUERY = 'query'

    @staticmethod
    def parse(poss_req: str) -> Optional[Request]:
        request = None
        try:
            poss_req = poss_req.strip().upper()
            request = Request[poss_req]
        except (UnsupportedOperation, KeyError):
            pass

        return request


T = TypeVar('T')

class Procedure(NamedTuple):
    """ Request message wrapped with positional and keyword args """
    request: Request
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def __call__(
        self,
        funct: Callable[..., T],
        *args: Any,
        self_first: bool = False,
        **kwargs: Any) -> T:
        """
        Calls the passed function, supplying in a merge of all of the args.
        The order of the positional and precedence of the keyword args is
        based on self_first
        """
        pos_args = (
            args + self.args
            if not self_first else
            self.args + args
        )
        key_args = ( # 2nd keywords override
            {**self.kwargs, **kwargs}
            if not self_first else
            {**kwargs, **self.kwargs}
        )

        return funct(*pos_args, **key_args)



class Message:
    """ Information that is passed between socket streams """
    payload: Any # pre-pickled data

    HEADER = struct.Struct("!I")

    def __init__(self, payload: Any):
        """
        Creates a message that can be packed; the payload should 
        not be pickled yet
        """
        self.payload = payload


    @staticmethod
    async def get(stream: asyncio.StreamReader) -> Message:
        """ Gets a message from the stream reader """
        header = await stream.readexactly(Message.HEADER.size)
        size, = Message.HEADER.unpack(header)

        data = await stream.readexactly(size)
        payload = pickle.loads(data)

        return Message(payload)


    def pack(self) -> bytes:
        """ Get the message with a metadata header for streams """
        data = pickle.dumps(self.payload)
        size = len(data)
        header = Message.HEADER.pack(size)
        return header + data


    def unwrap(self) -> Any:
        """ Return the payload or raise it as a decoded exception """
        if isinstance(self.payload, Exception):
            # potentially could have message of just random bytes
            raise self.payload
        else:
            return self.payload


    @staticmethod
    async def write(writer: asyncio.StreamWriter, payload: Any):
        """ Send a regular message or an exception through the stream """
        message = Message(payload)
        writer.write(message.pack())
        await writer.drain()


    @staticmethod
    async def read(reader: asyncio.StreamReader, as_type: type[T]) -> T:
        """ Get and unwrap bytes, as the expected type from the stream """
        message = await Message.get(reader)
        return cast(as_type, message.unwrap())


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

        if receiver is not None and as_type is not None:
            raise ValueError("Only as_type or receiver can be defined")

        if receiver is not None:
            return await receiver(self.reader)
        if as_type is not None:
            return await Message.read(self.reader, as_type)
        else:
            return cast(Any, None) # kind of hacky

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
    response = (await asyncio
        .get_event_loop()
        .run_in_executor(None, input, prompt))

    return response


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
        merged_args = { **vars(args), **read_config(args.config) }
        return merged_args


def version_check():
    if sys.version_info < (3,8):
        raise RuntimeError("Python version needs to be at least 3.8")
