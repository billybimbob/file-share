import sys
import asyncio
from collections import namedtuple


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
