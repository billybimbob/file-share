#!/usr/bin/env python3

from __future__ import annotations
from argparse import ArgumentParser
from typing import Any
from dataclasses import dataclass, field
from pathlib import Path

import asyncio as aio
import socket
import logging

from connection import (
    StreamPair, Message, Request,
    ainput, merge_config_args, version_check
)


@dataclass
class PeerState:
    """ Peer connection state """
    files: set[str] = field(default_factory=set)
    signal: aio.Event = field(default_factory=aio.Event)
    log: logging.Logger = field(default_factory=logging.getLogger)



class Indexer:
    """ Indexing server node that keeps track of file positions """
    port: int
    peers: dict[StreamPair, PeerState]
    updates: aio.Queue[StreamPair]

    PROMPT = "" \
        "1. List active peers\n" \
        "2. Kill Server (any value besides 1 also works)\n" \
        "Select an Option: "
    

    def __init__(self, port: int, *args: Any, **kwargs: Any):
        """
        Create the state structures for an indexing node; to start running the server, 
        run the function start_server
        """
        self.port = min(1, port)
        self.peers = {}
        self.updates = aio.Queue()


    def get_files(self) -> str:
        """ Gets all the unique files accross all peers """
        return '\n'.join({
            file
            for info in self.peers.values()
            for file in info.files
        })


    def get_location(self, filename: str) -> tuple[StreamPair]:
        """ Find the peer nodes associated with a file """
        locs = { # use set to find unique pairs
            pair
            for pair, info in self.peers.items()
            if filename in info.files
        }

        # make tuple to have predictable iter
        return tuple(locs)


    async def start_server(self):
        """
        Intilizes the indexer server and workers; awaiting on 
        this method will wait until the server is closed
        """
        def to_conneciton(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Indexeder closure as a stream callback """
            return self.connected(StreamPair(reader, writer))

        host = socket.gethostname()
        server = await aio.start_server(to_conneciton, host, self.port)

        async with server:
            if server.sockets:
                addr = server.sockets[0].getsockname()[0]
                logging.info(f'indexing server on {addr}')

                queries = aio.create_task(self.update_loop())
                aio.create_task(server.serve_forever())

                await aio.create_task(self.session())  
                queries.cancel()

        await server.wait_closed()
        logging.info("index server stopped")


    #region update queue handlers

    async def update_loop(self):
        """
        Handles query task put on the query queue, will run until cancelled
        """
        try:
            while True:
                update_loc = await self.updates.get()
                aio.create_task(self.update_files(update_loc)) # will call task_done

        except aio.CancelledError:
            pass


    async def update_files(self, pair: StreamPair):
        """ Query the given stream for updated file list """
        pair_state = self.peers[pair]
        files: list[str] = await Message.read(pair.reader, list)

        pair_state.files.intersection_update(files)

        self.updates.task_done()
        pair_state.signal.set()

    #endregion


    async def session(self):
        """ Cli for indexer """
        while option := await ainput(Indexer.PROMPT):
            if option == '1':
                peer_users = '\n'.join(
                    info.log.name
                    for pair, info in self.peers.items()
                    if not pair.writer.is_closing()
                )
                print('The peers connected are: ')
                print(f'{peer_users}\n')

            else:
                print('Exiting server')    
                break


    #region peer connection

    async def connected(self, pair: StreamPair):
        """ A connection with a specific peer """
        reader, writer = pair
        logger = logging.getLogger()

        try:
            username = await Message.read(reader, str)
            logger = logging.getLogger(username)

            self.peers[pair] = PeerState(log=default_logger(logger))

            info = writer.get_extra_info('peername')
            remote = socket.gethostbyaddr(info[0])
            logger.debug(f"connected to {username}: {remote}")

            await self.connect_loop(pair)

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


    async def connect_loop(self, pair: StreamPair):
        """ Request loop handler for each peer connection """
        while request := await Message.read(pair.reader, Request):
            if request == Request.GET_FILES:
                await self.send_files(pair)

            elif request == Request.UPDATE:
                await self.receive_update(pair)

            elif request == Request.QUERY:
                await self.query_files(pair)

            else:
                break

    
    async def send_files(self, pair: StreamPair):
        """
        Handles the get files request, and sends files to given socket
        """
        self.peers[pair].log.debug("getting all files in cluster")
        await Message.write(pair.writer, self.get_files())


    async def query_files(self, pair: StreamPair):
        """ Reply to stream with the peers that have specfied file """
        filename = await Message.read(pair.reader, str)
        self.peers[pair].log.debug(f'querying for file {filename}')

        await self.updates.join() # wait for no more updates

        assoc_peers: set[tuple[str, int]] = {
            loc.writer.get_extra_info('peername')[:2]
            for loc in self.get_location(filename)
        }

        await Message.write(pair.writer, assoc_peers)

    
    async def receive_update(self, pair: StreamPair):
        """ Notify update handler of a update task, and wait for completion """
        signal = self.peers[pair].signal
        signal.clear()
        await self.updates.put(pair)
        await signal.wait() # block stream access til finished

    #endregion



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

    indexer = Indexer(**args)
    aio.run(indexer.start_server())
