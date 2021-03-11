#!/usr/bin/env python3

from __future__ import annotations
from typing import Any
from dataclasses import dataclass, field

from argparse import ArgumentParser
from pathlib import Path

import asyncio as aio
import socket
import logging

from connection import (
    StreamPair, Message, Request,
    ainput, getpeerbystream, merge_config_args, version_check
)


@dataclass(frozen=True)
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

    PROMPT = (
        "1. List active peers\n"
        "2. List all files in the system\n"
        "3. Kill Server (any value besides 1 & 2 also work)\n"
        "Select an Option: ")
    

    def __init__(self, port: int, *args: Any, **kwargs: Any):
        """
        Create the state structures for an indexing node; to start running the server, 
        run the function start_server
        """
        self.port = max(1, port)
        self.peers = {}
        self.updates = aio.Queue()


    def get_files(self) -> list[str]:
        """ Gets all the unique files accross all peers in ascending order """
        return sorted({
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
            return self._connected(StreamPair(reader, writer))

        host = socket.gethostname()
        server = await aio.start_server(to_conneciton, host, self.port)

        async with server:
            if server.sockets:
                addr = server.sockets[0].getsockname()[0]
                logging.info(f'indexing server on {addr}')

                updates = aio.create_task(self._update_loop())
                aio.create_task(server.serve_forever())

                await aio.create_task(self._session())  
                updates.cancel()

        await server.wait_closed()
        logging.info("index server stopped")


    #region update queue handlers

    async def _update_loop(self):
        """
        Handles query task put on the query queue, will run until cancelled
        """
        try:
            while True:
                update_loc = await self.updates.get()
                aio.create_task(self._update_files(update_loc)) # will call task_done

        except aio.CancelledError:
            pass


    async def _update_files(self, pair: StreamPair):
        """ Query the given stream for updated file list """
        pair_state = self.peers[pair]
        files: frozenset[str] = await Message.read(pair.reader, frozenset)

        pair_state.files.clear()
        pair_state.files.update(files)

        self.updates.task_done()
        pair_state.signal.set()

    #endregion


    async def _session(self):
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

            elif option == '2':
                files = '\n'.join(self.get_files())
                print('The files on the system are:')
                print(f'{files}\n')

            else:
                break

        print('Exiting server')    


    #region peer connection

    async def _connected(self, pair: StreamPair):
        """ A connection with a specific peer """
        reader, writer = pair
        logger = logging.getLogger()

        try:
            username = await Message.read(reader, str)
            logger = logging.getLogger(username)

            self.peers[pair] = PeerState(log=default_logger(logger))

            info = getpeerbystream(writer)
            if info:
                remote = socket.gethostbyaddr(info[0])
                logger.debug(f"connected to {username}: {remote}")

                await self._connect_loop(pair)

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


    async def _connect_loop(self, pair: StreamPair):
        """ Request loop handler for each peer connection """

        while request := await Message.read(pair.reader, Request):
            if request == Request.GET_FILES:
                await self._send_files(pair)

            elif request == Request.UPDATE:
                await self._receive_update(pair)

            elif request == Request.QUERY:
                await self._query_file(pair)

            else:
                break

    
    async def _send_files(self, pair: StreamPair):
        """
        Handles the get files request, and sends files to given socket
        """
        self.peers[pair].log.debug("getting all files in cluster")
        await Message.write(pair.writer, self.get_files())


    async def _receive_update(self, pair: StreamPair):
        """ Notify update handler of a update task, and wait for completion """
        signal = self.peers[pair].signal
        signal.clear()
        await self.updates.put(pair)
        await signal.wait() # block stream access til finished


    async def _query_file(self, pair: StreamPair):
        """ Reply to stream with the peers that have specfied file """
        filename = await Message.read(pair.reader, str)
        self.peers[pair].log.debug(f'querying for file {filename}')

        await self.updates.join() # wait for no more updates

        peers = (
            getpeerbystream(loc) 
            for loc in self.get_location(filename)
        )
        assoc_peers: set[tuple[str, int]] = {
            peer
            for peer in peers
            if peer is not None
        }

        await Message.write(pair.writer, assoc_peers)

    #endregion



def init_log(log: str, *args: Any, **kwargs: Any):
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
    args.add_argument("-l", "--log", default='indexer.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")

    args = args.parse_args()
    args = merge_config_args(args)

    init_log(**args)

    indexer = Indexer(**args)
    aio.run(indexer.start_server())

