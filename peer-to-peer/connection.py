#!/usr/bin/env python3

from __future__ import annotations
from typing import Dict, Any, NamedTuple, TypeVar, Type, cast
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

#region request constants

GET_FILES = 'get_files_list'
DOWNLOAD = 'download'
SUCCESS = 'success'
RETRY = 'retry'
QUERY = 'query'

#endregion


#region message passing

class Message(NamedTuple):
    """ Information that is passed between socket streams """
    payload: Any # pre-pickled data

    T = TypeVar('T')
    HEADER = struct.Struct("!I")


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
        """ Get the payload or raise it as a decoded exception """
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
    async def read(reader: asyncio.StreamReader, as_type: Type[T]) -> T:
        """ Get and unwrap bytes, as the expected type from the stream """
        message = await Message.get(reader)
        return cast(as_type, message.unwrap())

#endregion


class StreamPair(NamedTuple):
    """ Stream pairing """
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter



async def ainput(prompt: str='') -> str:
    """ Async version of user input """

    await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: sys.stdout.write(prompt)
    )

    return await asyncio.get_event_loop().run_in_executor(
        None, sys.stdin.readline
    )


# def shallow_obj(map: Dict[str, Any]) -> object:
#     """
#     Convert a dictionary into a python object, where each key is an attribute
#     """
#     return namedtuple('obj', map.keys())(*map.values())


def read_config(filename: str) -> Dict[str, Any]:
    """ Parse an ini file into a dictionary, ignoring sections """
    if not Path(filename).exists():
        raise IOError("Config file does not exist")

    conf = ConfigParser()
    conf.read(filename)

    args: Dict[str, Any] = {}
    for section in conf.sections():
        for arg in conf[section]:
            args[arg] = conf[section][arg]
            if args[arg].isnumeric():
                args[arg] = int(args[arg])

    return args


def merge_config_args(args: Namespace) -> Dict[str, Any]:
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
