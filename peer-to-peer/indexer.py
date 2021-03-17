#!/usr/bin/env python3

from __future__ import annotations
from typing import Any, NamedTuple, Optional
from dataclasses import dataclass, field

from argparse import ArgumentParser
from pathlib import Path

import asyncio as aio
import socket
import logging

from connection import (
    Procedure, StreamPair, Message, Request,
    ainput, getpeerbystream, merge_config_args, version_check
)


@dataclass(frozen=True)
class PeerState:
    """ Peer connection state """
    loc: tuple[str,int]
    files: set[str] = field(default_factory=set)
    signal: aio.Event = field(default_factory=aio.Event)
    log: logging.Logger = field(default_factory=logging.getLogger)


class PeerUpdate(NamedTuple):
    """ Pending changes to update the a given peer's state """
    peer: StreamPair
    args: Optional[dict[str, Any]] = None


class Login(NamedTuple):
    """ Initial peer identification """
    user: str
    host: str
    port: int


class Indexer:
    """ Indexing server node that keeps track of file positions """
    port: int
    peers: dict[StreamPair, PeerState]
    updates: aio.PriorityQueue[tuple[int, PeerUpdate]]

    DELETE_PRIORITY = 1
    UPDATE_PRIORITY = 2

    PROMPT = (
        "1. List active peers\n"
        "2. List all files in the system\n"
        "3. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")
    

    def __init__(self, port: int, **_):
        """
        Create the state structures for an indexing node; to start running the server, 
        run the function start_server
        """
        self.port = max(1, port)
        self.peers = {}


    def get_files(self) -> frozenset[str]:
        """ Gets all the unique files accross all peers """
        return frozenset(
            file
            for info in self.peers.values()
            for file in info.files
        )


    def get_location(self, filename: str) -> set[tuple[str, int]]:
        """ Find the peer nodes associated with a file """
        locs = {
            info.loc
            for info in self.peers.values()
            if filename in info.files
        }

        return locs


    async def start_server(self):
        """
        Initializes the indexer server and workers; awaiting on 
        this method will wait until the server is closed
        """
        # async sync state needs to be init from async context
        self.updates = aio.PriorityQueue()

        def to_connection(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Indexer closure as a stream callback """
            return self._peer_connected(StreamPair(reader, writer))

        try:
            host = socket.gethostname()
            server = await aio.start_server(to_connection, host, self.port, start_serving=False)

            async with server:
                if server.sockets:
                    addr = server.sockets[0].getsockname()[0]
                    logging.info(f'indexing server on {addr}')

                    updates = aio.create_task(self._update_loop())

                    await server.start_serving()
                    await aio.create_task(self._session())  

                    updates.cancel()

            await server.wait_closed()

        except Exception as e:
            logging.exception(e)
            for task in aio.all_tasks():
                task.cancel()

        logging.info("index server stopped")



    #region update queue handlers

    async def _update_loop(self):
        """
        Handles update tasks put on the update queue, will run until cancelled
        """
        try:
            while True:
                priority, update_info = await self.updates.get()
                peer, args = update_info

                if priority == Indexer.UPDATE_PRIORITY and args:
                    self._update_files(peer, **args) # will call task_done
                elif priority == Indexer.DELETE_PRIORITY:
                    self._delete_peer(peer)
                else:
                    logging.error(f'got unknown priority or update missing args')

        except aio.CancelledError:
            pass


    def _update_files(self, peer: StreamPair, files: frozenset[str]):
        """ Query the given stream for updated file list """
        if peer not in self.peers:
            return

        peer_state = self.peers[peer]

        peer_state.files.clear()
        peer_state.files.update(files)

        self.peers[peer].log.debug("got updated files")
        self.updates.task_done()
        peer_state.signal.set()

    
    def _delete_peer(self, peer: StreamPair):
        """ Removes a peer entry, should only be called after peer ends connection """
        if peer in self.peers:
            del self.peers[peer]
        
        self.updates.task_done()
    

    #endregion


    async def _session(self):
        """ Cli for indexer """
        while option := await ainput(Indexer.PROMPT):
            if option == '1':
                peer_users = '\n'.join(
                    str(peer)
                    for peer in (
                        getpeerbystream(pair)
                        for pair in self.peers.keys()
                        if not pair.writer.is_closing())
                    if peer is not None
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

    async def _peer_connected(self, peer: StreamPair):
        """ A connection with a specific peer """
        reader, writer = peer
        logger = logging.getLogger()

        try:
            login = await Message.read(reader, Login)
            username, host, port = login
            loc = (host, port)
            logger = logging.getLogger(username)

            self.peers[peer] = PeerState(loc, log=default_logger(logger))

            remote = getpeerbystream(writer)
            if remote:
                logger.debug(f"connected to {username}: {remote}")

                await self._connect_loop(peer)

        except aio.IncompleteReadError:
            pass

        except Exception as e:
            logger.exception(e)
            await Message.write(writer, e)

        finally:
            if not reader.at_eof():
                writer.write_eof()

            logger.debug('ending connection')
            writer.close()

            await self.updates.put((Indexer.DELETE_PRIORITY, PeerUpdate(peer)))
            await writer.wait_closed()



    async def _connect_loop(self, peer: StreamPair):
        """ Request loop handler for each peer connection """

        while procedure := await Message.read(peer.reader, Procedure):
            request = procedure.request

            if request is Request.GET_FILES:
                await procedure(self._send_files, peer)

            elif request is Request.UPDATE:
                await procedure(self._receive_update, peer)

            elif request is Request.QUERY:
                await procedure(self._query_file, peer)

            else:
                break



    async def _send_files(self, peer: StreamPair):
        """
        Handles the get files request, and sends files to given socket
        """
        self.peers[peer].log.debug("getting all files in cluster")
        await self.updates.join() # wait for no more file updates
        await Message.write(peer.writer, self.get_files())


    async def _receive_update(self, peer: StreamPair, **update_args: Any):
        """ Notify update handler of a update task, and wait for completion """
        self.peers[peer].log.debug("updating file info")
        signal = self.peers[peer].signal
        signal.clear()
        await self.updates.put((Indexer.UPDATE_PRIORITY, PeerUpdate(peer, update_args)))
        await signal.wait() # block stream access til finished


    async def _query_file(self, peer: StreamPair, filename: str):
        """ Reply to stream with the peers that have specified file """
        self.peers[peer].log.debug(f'querying for file {filename}')
        await self.updates.join() # wait for no more file updates
        await Message.write(peer.writer, self.get_location(filename))

    #endregion



def init_log(log: str, **_):
    """ Specifies logging format and location """
    log_path = Path(f'./{log}')
    log_path.parent.mkdir(exist_ok=True, parents=True)
    log_settings = {
        'format': "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s: %(message)s",
        'datefmt': "%H:%M:%S",
        'level': logging.DEBUG
    }

    if not log_path.exists() or log_path.is_file():
        logging.basicConfig(filename=log, filemode='w', **log_settings)
    else: # just use stdout
        logging.basicConfig(**log_settings)


def default_logger(log: logging.Logger) -> logging.Logger:
    """ Settings for all created loggers """
    log.setLevel(logging.DEBUG)
    return log


if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser(description="creates and starts an indexing server node")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-l", "--log", default='indexer.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")

    args = args.parse_args()
    args = merge_config_args(args)

    init_log(**args)

    indexer = Indexer(**args)
    aio.run(indexer.start_server())

