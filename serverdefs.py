from typing import Dict, Any, NamedTuple
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

messenger = struct.Struct("?1023s")
MESSAGE_SIZE = messenger.size
MAX_PAYLOAD = MESSAGE_SIZE-1 # leave one byte for bool

class Message(NamedTuple):
    """ Information that is passed between socket streams """
    message: str
    is_exception: bool = False

    def to_bytes(self) -> bytes:
        """ Convert message in byte, struct form for streams """
        return messenger.pack(self.is_exception, self.message.encode())


def unpack_to_message(bytes: bytes) -> Message:
    """
    Take a stream of bytes and try to convert to a message. If the bytes
    are not in the correct format, this will throw an error
    """
    exception, payload = messenger.unpack(bytes)
    return Message(payload.decode().rstrip('\x00'), exception)

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