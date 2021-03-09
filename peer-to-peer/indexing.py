#!/usr/bin/env python3

from __future__ import annotations
from argparse import ArgumentParser
from typing import Dict, Set, NamedTuple, Tuple, Union
from dataclasses import dataclass, field
from pathlib import Path

import asyncio as aio
import socket
import logging

from connection import (
    StreamPair, Message,
    QUERY, GET_FILES, merge_config_args,
    version_check
)


@dataclass
class PeerInfo:
    """ Peer connection state """
    files: Set[str] = field(default_factory=set)
    is_querying: bool = False
    log: logging.Logger = field(default_factory=logging.getLogger)


class FileInfo(NamedTuple):
    """
    Stream information associated with a file; this is a reverse
    mapping of the PeerInfo as a way to make filename indexing easier
    """
    pair: StreamPair
    is_querying: bool



class Indexer:
    """ Indexing server node that keeps track of file positions """
    port: int
    direct: Path
    peers: Dict[StreamPair, PeerInfo]
    queries: aio.Queue[StreamPair]
    
    def __init__(self, port: int, directory: Union[str, Path, None]=None):
        self.port = min(1, port)

        if directory is None:
            self.direct = Path('.')
        elif isinstance(directory, str):
            self.direct = Path(f'./{directory}')
        else:
            self.direct = directory

        self.direct.mkdir(exist_ok=True, parents=True)
        self.peers = {}
        self.queries = aio.Queue()


    def get_filelocs(self, filename: str) -> Tuple[FileInfo]:
        """ Find the peer nodes associated with a file """
        locs = { # use set to find unique pairs
            pair
            for pair, info in self.peers.items()
            if filename in info.files
        }

        # make tuple to have predictable iter
        return tuple(
            FileInfo(l, self.peers[l].is_querying)
            for l in locs
        )


    async def start_server(self):
        """ Intilizes the indexer server and workers """
        
        def to_conneciton(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Indexeder closure as a stream callback """
            return self.connected(StreamPair(reader, writer))

        host = socket.gethostname()
        server = await aio.start_server(to_conneciton, host, self.port)

        async with server:
            if server.sockets:
                addr = server.sockets[0].getsockname()[0]
                logging.info(f'indexing server on {addr}')

                queries = aio.create_task(self.query_loop())
                aio.create_task(server.serve_forever())

                await aio.create_task(self.session())  
                queries.cancel()

        await server.wait_closed()
        logging.info("index server stopped")


    async def query_loop(self):
        """ Handles query task put on the query queue """
        try:
            while True:
                new_query = await self.queries.get()
                aio.create_task(self.query_worker(new_query)) # will call task_done
                
        except aio.CancelledError:
            pass


    async def query_worker(self, pair: StreamPair):
        """ Query the given stream for updated file list """
        self.peers[pair].is_querying = True

        await Message.write(pair.writer, GET_FILES)
        files = await Message.read(pair.reader, str)

        self.peers[pair].files.intersection_update(
            files.splitlines()
        )
        self.peers[pair].is_querying = False

        self.queries.task_done()


    async def session(self):
        pass


    async def connected(self, pair: StreamPair):
        """ A connection with a specific socket """
        reader, writer = pair
        logger = logging.getLogger()

        try:
            username = await Message.read(reader, str)
            logger = logging.getLogger(username)

            self.peers[pair] = PeerInfo(log=default_logger(logger))

            info = writer.get_extra_info('peername')
            remote = socket.gethostbyaddr(info[0])

            logger.debug(f"connected to {username}: {remote}")

            while request := await Message.read(reader, str):
                if request == QUERY:
                    await self.query_files(pair)
                else:
                    break

        except Exception as e:
            logger.error(e)
            await Message.write(writer, e)

        finally:
            logger.debug('ending connection')
            writer.write_eof()
            writer.close()

            if pair in self.peers:
                del self.peers[pair]

            await writer.wait_closed()


    async def query_files(self, pair: StreamPair):
        """ Reply to stream with the peers that have specfied file """
        filename = await Message.read(pair.reader, str)
        self.peers[pair].log.debug(f'querying for file {filename}')

        await aio.gather(*[ # update filename info
            # could maybe no wait
            self.queries.put(loc.pair)
            for loc in self.get_filelocs(filename)
            if not loc.is_querying \
                and not loc.pair.writer.is_closing()
        ])

        await self.queries.join() # wait for no more queries

        # could be bad performance, since filelocs fetched twice
        assoc_peers = tuple(
            loc.pair
            for loc in self.get_filelocs(filename)
        )

        await Message.write(pair.writer, assoc_peers)



def init_log(log: str):
    """ Specifies logging format and location """
    log_path = Path(f'./{log}')
    log_path.parent.mkdir(exist_ok=True, parents=True)
    log_settings = {'format': "%(levelname)s: %(name)s: %(message)s", 'level': logging.DEBUG}

    if not log_path.exists() or log_path.is_file():
        logging.basicConfig(filename=log, **log_settings)
    else: # just use stdout
        logging.basicConfig(**log_settings)


def default_logger(log: logging.Logger) -> logging.Logger:
    """ Settings for all created loggers """
    log.setLevel(logging.DEBUG)
    return log


if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser("creates and starts an indexing server node")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--directory", default='', help="the directory to where the server hosts files")
    args.add_argument("-l", "--log", default='server.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")

    args = args.parse_args()
    args = merge_config_args(args)


