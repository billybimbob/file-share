from typing import Dict, Any, Union, NamedTuple
from collections import namedtuple

from pathlib import Path
from configparser import ConfigParser
from argparse import Namespace

import sys
import struct
import asyncio

MAX_PAYLOAD = 1024

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

    full = struct.Struct(f"!{MAX_PAYLOAD-1}s?") # one byte for the bool
    short = struct.Struct("!255p?")


    def to_bytes(self) -> bytes:
        """ Convert message to byte, struct form for streams """
        if isinstance(self.message, bytes):
            encoded = self.message
        else:
            encoded = self.message.encode()

        if len(encoded) < Message.short.size:
            messenger = Message.short
        else:
            messenger = Message.full

        return messenger.pack(encoded, self.is_exception)


    def unwrap(self, decode: bool=True) -> str:
        """ Get the message, or raise it as an exception """
        if isinstance(self.message, bytes) and decode:
            decoded = self.message.decode().rstrip('\x00')
        else:
            decoded = self.message

        if not self.is_exception:
            return decoded
        else:
            raise RuntimeError(decoded)


    @staticmethod
    async def write(writer: asyncio.StreamWriter, info: Union[str, bytes], error=False):
        """ Send message as bytes or also send an error """
        if not isinstance(info, str) and not isinstance(info, bytes):
            info = str(info)
        message = Message(info, error)
        writer.write(message.to_bytes())
        await writer.drain()


    @staticmethod
    async def read_short(reader: asyncio.StreamReader, decode: bool=True) -> Union[str, bytes]:
        """ Get a short message from the stream """
        data = await reader.readexactly(Message.short.size)
        unpack = Message.short.unpack(data)

        return Message(*unpack).unwrap(decode)


    @staticmethod
    async def read_full(reader: asyncio.StreamReader, decode: bool=True) -> Union[str, bytes]:
        """ Get a long message from the stream """
        data = await reader.readexactly(Message.full.size)
        unpack = Message.full.unpack(data)

        return Message(*unpack).unwrap(decode)

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


def shallow_obj(map: Dict) -> object:
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