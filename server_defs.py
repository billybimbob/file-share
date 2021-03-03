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
    message: Union[str, bytes]
    is_exception: bool = False

    HEADER = struct.Struct("?I")


    def to_stream(self) -> bytes:
        """ Convert message to byte struct form for streams """
        if isinstance(self.message, bytes):
            encoded = self.message
        else:
            encoded = self.message.encode()

        size = len(encoded)
        header = Message.HEADER.pack(self.is_exception, size)

        return header + encoded


    def unwrap(self, decode: bool=True) -> Union[str, bytes]:
        """ Get the message, or raise it as an exception """
        if isinstance(self.message, bytes) and (decode or self.is_exception):
            decoded = self.message.decode()
        else:
            decoded = self.message

        if self.is_exception:
            raise RuntimeError(decoded)
        else:
            return decoded


    @staticmethod
    async def write(writer: asyncio.StreamWriter, info: Union[str, bytes], error: bool=False):
        """ Send message as bytes or also send an error """
        message = Message(info, error)
        writer.write(message.to_stream())
        await writer.drain()


    @staticmethod
    async def read_message(reader: asyncio.StreamReader) -> Message:
        """ Get a wrapped Message from the stream """
        header = await reader.readexactly(Message.HEADER.size)
        exception, size = Message.HEADER.unpack(header)
        payload = await reader.readexactly(size)

        return Message(payload, exception)


    @staticmethod
    async def read(reader: asyncio.StreamReader) -> str:
        """ Get and unwrap a str from the stream """
        message = await Message.read_message(reader)
        return cast(str, message.unwrap())


    @staticmethod
    async def read_raw(reader: asyncio.StreamReader) -> bytes:
        """ Get and unwrap bytes from the stream """
        message = await Message.read_message(reader)
        return cast(bytes, message.unwrap(False))

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
