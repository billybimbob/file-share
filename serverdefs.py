from typing import Dict, Any
from collections import namedtuple

from pathlib import Path
from configparser import ConfigParser
from argparse import Namespace

import sys
import asyncio


CHUNK_SIZE = 1024

#region request constants

GET_FILES = 'get_files_list'
DOWNLOAD = 'download'
SUCCESS = 'success'
RETRY = 'retry'

#endregion

StreamPair = namedtuple('StreamPair', ['reader', 'writer'])


async def ainput(prompt: str) -> str:
    """Async version of user input"""

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
    """
    Parse an ini file into a Python object where the attributes are
    the arguments, similar to the Namespace object in argparse
    """
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
    """If config file specified, merge in config arguments as a dictionary"""
    if not args.config:
        return vars(args)
    else:
        # config overrides command args
        merged_args = { **vars(args), **read_config(args.config) }
        return merged_args