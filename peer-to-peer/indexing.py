#!/usr/bin/env python3

from __future__ import annotations
from typing import Dict, Set, Tuple, Union
from pathlib import Path

import asyncio as aio
import socket
import logging

from connection import (
    StreamPair, Message,
    QUERY, GET_FILES
)



class Indexer:
    port: int
    direct: Path
    peers: Dict[StreamPair, Tuple[Set[str], logging.Logger]]

    
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


    def get_filelocs(self) -> Dict[str, Set[StreamPair]]:
        """ Mapping to find the peer nodes associated with a file """
        locs: Dict[str, Set[StreamPair]] = {}

        for pair, info in self.peers.items():
            fnames = info[0]
            for f in fnames:
                if f not in locs:
                    locs[f] = set()

                locs[f].add(pair)

        return locs


    async def start_server(self):
        host = socket.gethostname()
        def to_conneciton(reader: aio.StreamReader, writer: aio.StreamWriter):
            return self.connected(StreamPair(reader, writer))

        server = await aio.start_server(to_conneciton, host, self.port)
        async with server:
            if server.sockets:
                addr = server.sockets[0].getsockname()
                addr = addr[0]
                logging.info(f'indexing server on {addr}')

                aio.create_task(server.serve_forever())
                await aio.create_task(self.session())  

        await server.wait_closed()
        logging.info("index server stopped")


    async def session(self):
        pass


    async def connected(self, pair: StreamPair):
        reader, writer = pair
        logger = logging.getLogger()

        try:
            username = await Message.read(reader, str)
            logger = logging.getLogger(username)

            self.peers[pair] = set(), logger

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
        filename = await Message.read(pair.reader, str)
        filelocs = self.get_filelocs()

        if filename not in filelocs:
            poss_locs: Tuple[StreamPair] = tuple()
        else:
            poss_locs = tuple(filelocs[filename])

        await aio.gather(*[ # potential buffer issue
            Message.write(loc.writer, GET_FILES)
            for loc in poss_locs
        ])

        new_files_info = await aio.gather(*[
            Message.read(loc.reader, str)
            for loc in poss_locs
        ])


        for nfi, loc in zip(new_files_info, poss_locs):
            self.peers[loc][0].intersection_update(nfi.split())

        # could be bad performance
        filelocs = self.get_filelocs()
        assoc_peers = frozenset(filelocs[filename])

        await Message.write(pair.writer, assoc_peers)


