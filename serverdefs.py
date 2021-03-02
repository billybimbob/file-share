from typing import Dict, Any, Union, NamedTuple
from collections import namedtuple

from pathlib import Path
from configparser import ConfigParser
from argparse import Namespace

import sys
import struct
import asyncio

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

    header = struct.Struct("?I")


    def to_bytes(self) -> bytes:
        """ Convert message to byte, struct form for streams """
        if isinstance(self.message, bytes):
            encoded = self.message
        else:
            encoded = self.message.encode()

        size = len(encoded)
        header = Message.header.pack(self.is_exception, size)

        return header + encoded


    def unwrap(self, decode: bool=True) -> Union[str, bytes]:
        """ Get the message, or raise it as an exception """
        if isinstance(self.message, bytes) and decode:
            decoded = self.message.decode()
        else:
            decoded = self.message

        if not self.is_exception:
            return decoded
        else:
            raise RuntimeError(decoded)


    @staticmethod
    async def write(writer: asyncio.StreamWriter, info: Union[str, bytes], error: bool=False):
        """ Send message as bytes or also send an error """
        message = Message(info, error)
        writer.write(message.to_bytes())
        await writer.drain()


    @staticmethod
    async def read(reader: asyncio.StreamReader, decode: bool=True) -> Union[str, bytes]:
        """ Get a short message from the stream """
        header = await reader.readexactly(Message.header.size)

        exception, size = Message.header.unpack(header)
        payload = await reader.readexactly(size)

        return Message(payload, exception).unwrap(decode)

#endregion


class StreamPair(NamedTuple):
    """ Stream pairing """
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


async def ainput(prompt: str) -> str:
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
    """ Parse an ini file into a dictionary """
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