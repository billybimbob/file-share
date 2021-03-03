from __future__ import annotations
from typing import Dict, Any, Union, NamedTuple, cast
from collections import namedtuple

from pathlib import Path
from configparser import ConfigParser
from argparse import Namespace

import sys
import struct
import asyncio


# Max amount read for files
CHUNK_SIZE = 1024

#region request constants

GET_FILES = 'get_files_list'
DOWNLOAD = 'download'
SUCCESS = 'success'
RETRY = 'retry'

#endregion


#region message passing

class Message(NamedTuple):
    """ Information that is passed between socket streams """
    message: bytes
    is_exception: bool = False

    HEADER = struct.Struct("!?I")


    def with_header(self) -> bytes:
        """ Get the message with a metadata header for streams """
        size = len(self.message)
        header = Message.HEADER.pack(self.is_exception, size)
        return header + self.message


    def unwrap(self) -> bytes:
        """ Get the bytes message or raise it as a decoded exception """
        if self.is_exception:
            # potentially could have message of just random bytes
            raise RuntimeError(self.message.decode())
        else:
            return self.message


    @staticmethod
    async def write(writer: asyncio.StreamWriter, info: Any):
        """
        Send a regular message through the stream, any non-bytes value will be
        converted to a string and encoded
        """
        if isinstance(info, bytes):
            message = Message(info)
        else: # str types should str to the same val
            encoded = str(info).encode()
            message = Message(encoded)

        writer.write(message.with_header())
        await writer.drain()

    
    @staticmethod
    async def write_error(writer: asyncio.StreamWriter, error: Exception):
        """ Send an error message encoded through the stream """
        encoded = str(error).encode()
        message = Message(encoded, True)
        writer.write(message.with_header())
        await writer.drain()


    @staticmethod
    async def read_bytes(reader: asyncio.StreamReader) -> bytes:
        """ Get and unwrap bytes from the stream """
        header = await reader.readexactly(Message.HEADER.size)
        exception, size = Message.HEADER.unpack(header)
        payload = await reader.readexactly(size)

        return Message(payload, exception).unwrap()


    @staticmethod
    async def read(reader: asyncio.StreamReader) -> str:
        """ Get and unwrap a str from the stream """
        message = await Message.read_bytes(reader)
        return message.decode()

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


def shallow_obj(map: Dict[str, Any]) -> object:
    """
    Convert a dictionary into a python object, where each key is an attribute
    """
    return namedtuple('obj', map.keys())(*map.values())


def read_config(filename: str) -> Dict[str, Any]:
    """ Parse an ini file into a dictionary, ignoring sections """
    if not Path(filename).exists():
        raise IOError("Config file does not exist")

    conf = ConfigParser()
    conf.read(filename)

    args = {}
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
