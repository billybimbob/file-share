#!/usr/bin/env python3

from __future__ import annotations
from time import time
from typing import Any, Optional, TypedDict
from dataclasses import dataclass, field

from argparse import ArgumentParser
from pathlib import Path

import asyncio as aio
import socket
import logging

from topology import Graph
from connection import (
    Procedure, Query, QueryHit, StreamPair, Message, Request, Login,
    ainput, getpeerbystream, merge_config_args, version_check
)



@dataclass(frozen=True)
class WeakState:
    """ Weak peer connection state """
    loc: tuple[str,int]
    files: set[str] = field(default_factory=set)
    log: logging.Logger = field(default_factory=logging.getLogger)


@dataclass
class SuperState:
    """ Strong peer connection state """
    receiver: Optional[StreamPair] = None
    sender: Optional[StreamPair] = None


class RequestCall:
    """ Pending changes to update the a given peer's state """
    _conn: StreamPair
    _args: Args

    class Args(TypedDict):
        """ Args for StreamPeer requests """
        req_type: Request

    def __init__(self, request: Request, conn: StreamPair, **kwargs: Any):
        self._conn = conn
        self._args = RequestCall.Args(req_type=request, **kwargs)

    def __call__(self):
        return self._conn.request(**self._args)


class SuperPeer:
    """ Super peer node """
    _id: str
    _port: int

    _supers: dict[str, SuperState]
    _min_supers: Graph[str]

    _weaks: dict[StreamPair, WeakState]
    _remote_files: dict[StreamPair, frozenset[str]]

    _queries: dict[str, StreamPair]
    _requests: aio.Queue[RequestCall]

    # WEAK_PRIORITY = 1
    # STRONG_PRIORITY = 2

    PROMPT = (
        "1. List active weak peers\n"
        "2. List active strong peers\n"
        "3. List all files in the system\n"
        "4. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")


    def __init__(self, id: str, port: int, super_graph: Graph[str]):
        """
        Create the state structures for a strong peer node; to start running the server, 
        run the function start_server
        """
        self._id = id
        self._port = max(1, port)
        self._min_supers = super_graph.min_span()

        self._weaks = dict()
        self._queries = dict()
        self._supers = {
            neighbor.value: SuperState()
            for neighbor in super_graph.get_vertex(id).neighbors()
        }


    def get_files(self) -> frozenset[str]:
        """ Gets all the unique files accross all peers """
        locals = frozenset(
            file
            for info in self._weaks.values()
            for file in info.files
        )

        remotes = frozenset(
            file
            for remote in self._remote_files.values()
            for file in remote
        )

        return locals | remotes


    def get_location(self, filename: str) -> frozenset[tuple[str, int]]:
        """ Find the peer nodes associated with a file """
        locs = frozenset(
            info.loc
            for info in self._weaks.values()
            if filename in info.files
        )

        return locs


    async def start_server(self):
        """
        Initializes the indexer server and workers; awaiting on 
        this method will wait until the server is closed
        """
        # async sync state needs to be init from async context
        self._requests = aio.Queue()

        async def to_receiver(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Switch to determine which receiver to use """
            conn = StreamPair(reader, writer)
            login = await Message[Login].read(reader) # TODO: add try except

            if login.is_super:
                await self._super_receiver(login, conn)
            else:
                await self._weak_receiver(login, conn)

        try:
            host = socket.gethostname()
            server = await aio.start_server(
                to_receiver, host, self._port, start_serving=False
            )

            async with server:
                if not server.sockets:
                    return

                addr = server.sockets[0].getsockname()[0]
                logging.info(f'super server on {addr}')

                caller = aio.create_task(self._request_caller())

                await server.start_serving()
                await self._session()

                caller.cancel()

            await server.wait_closed()

        except Exception as e:
            logging.exception(e)
            for task in aio.all_tasks():
                task.cancel()

        finally:
            logging.info("index server stopped")


    async def _super_connects(self):
        pass


    async def _session(self):
        """ Cli for indexer """
        while option := await ainput(SuperPeer.PROMPT):
            if option == '1':
                peer_users = '\n'.join(
                    str(peer)
                    for peer in (
                        getpeerbystream(pair)
                        for pair in self._weaks.keys()
                        if not pair.writer.is_closing())
                    if peer is not None
                )
                print('The peers connected are: ')
                print(f'{peer_users}\n')

            elif option == '2':
                pass

            elif option == '3':
                files = '\n'.join(self.get_files())
                print('The files on the system are:')
                print(f'{files}\n')

            else:
                break

        print('Exiting server')


    async def _request_caller(self):
        """ Handles sending all requests to specified targets"""
        async def request_task(req_call: RequestCall):
            await req_call()
            self._requests.task_done()

        try:
            while True:
                req_call = await self._requests.get()
                 # keep eye on if need locking
                aio.create_task(request_task(req_call))

        except aio.CancelledError:
            pass


    async def _super_receiver(self, login: Login, conn: StreamPair):
        """ Server-side connection with another strong peer """
        reader, writer = conn
        logger = logging.getLogger()
        writer.write_eof() # do not use server for writing

        async def query(query: Query):
            """ Actions for strong query requests """
            start = time()

            state = self._supers[login.id]
            if state.sender is None:
                logger.error('sender is not init')
                return

            self._queries[query.id] = state.sender
            await self._local_query(query)

            elapsed = time() - start
            new_query = query.elapsed(elapsed)
            await self._forward_query(new_query)


        async def hit(hit: QueryHit):
            """ Actions for super hit requests """
            if hit.id not in self._queries:
                logging.error(f'hit for {hit.id} does not exist')
                return

            src = self._queries[hit.id]
            await self._requests.put(
                RequestCall(Request.HIT, src, hit=hit)
            )


        async def update(files: frozenset[str]):
            """ Actions for super update requests """
            self._remote_files[conn] = files
            await self._forward_update(conn, files)

        try:
            while procedure := await Message[Procedure].read(reader):
                request = procedure.request

                if request is Request.QUERY:
                    await procedure(query)

                elif request is Request.HIT:
                    await procedure(hit)

                elif request is Request.UPDATE:
                    await procedure(update)

                else:
                    raise ValueError('Did not get an expected request')

        except Exception as e:
            logger.exception(e)
            await Message.write(writer, e)

        finally:
            logger.debug('ending connection')
            writer.close()
            await writer.wait_closed() # delete super state



    async def _weak_receiver(self, login: Login, conn: StreamPair):
        """ Server-side connection with a weak peer """
        reader, writer = conn
        logger = logging.getLogger()

        async def query(filename: str):
            """ Actions for weak query requests """
            conn_info = getpeerbystream(conn)
            curr_time = time()

            id = f'{conn_info[0]}:{conn_info[1]}:{filename}:{curr_time}'
            timeout = 30 # TODO: make param
            weak_query = Query(id, filename, timeout)

            self._queries[weak_query.id] = conn
            await self._local_query(weak_query)
            await self._forward_query(weak_query) 
        

        async def update(files: frozenset[str]):
            """ Actions for weak update requests """
            weak = self._weaks[conn]
            weak.files.clear()
            weak.files.update(files)
            await self._forward_update(conn, files)

        try:
            while procedure := await Message[Procedure].read(reader):
                request = procedure.request

                if request is Request.QUERY:
                    await procedure(query)
                elif request is Request.UPDATE:
                    await procedure(update)
                else:
                    raise ValueError('Did not get an expected request')

        except Exception as e:
            logger.exception(e)
            await Message.write(writer, e)

        finally:
            logger.debug('ending connection')
            writer.close()
            await writer.wait_closed() # delete super state


    async def _local_query(self, query: Query):
        """
        Checks if query filename exists locally and keeps track of query 
        alive timeout
        """
        locs = self.get_location(query.filename)

        async def alive_timer():
            """ Timer to autoremove query if dead """
            await aio.sleep(query.alive_time)
            del self._queries[query.id]

        if locs:
            src = self._queries[query.id]
            hit = QueryHit(query.id, locs)
            await self._requests.put(
                RequestCall(Request.HIT, src, hit=hit)
            )

        aio.create_task(alive_timer())


    async def _forward_query(self, query: Query):
        src = self._queries[query.id]
        for id, st in self._supers.items():
            if (st.sender is None
                or not self._min_supers.has_connection(self._id, id)
                or src == st.receiver):
                continue

            await self._requests.put(
                RequestCall(Request.QUERY, st.sender, query=query)
            )


    async def _forward_update(self, src: StreamPair, files: frozenset[str]):
        for id, st in self._supers.items():
            if (st.sender is None
                or not self._min_supers.has_connection(self._id, id)
                or st.receiver == src):
                continue

            await self._requests.put(
                RequestCall(Request.UPDATE, st.sender, files=files)
            )


    #region peer connection

    # async def _peer_connected(self, peer: StreamPair):
    #     """ A connection with a specific peer """
    #     reader, writer = peer
    #     logger = logging.getLogger()

    #     try:
    #         login = await Message[Login].read(reader)
    #         username, host, port = login
    #         loc = (host, port)
    #         logger = logging.getLogger(username)

    #         self._peers[peer] = WeakState(loc, log=default_logger(logger))

    #         remote = getpeerbystream(writer)
    #         if remote:
    #             logger.debug(f"connected to {username}: {remote}")

    #             await self._connect_loop(peer)

    #     except aio.IncompleteReadError:
    #         pass

    #     except Exception as e:
    #         logger.exception(e)
    #         await Message.write(writer, e)

    #     finally:
    #         if not reader.at_eof():
    #             writer.write_eof()

    #         logger.debug('ending connection')
    #         writer.close()

    #         await self._requests.put((Indexer.DELETE_PRIORITY, RequestCall(peer)))
    #         await writer.wait_closed()


    # async def _send_files(self, peer: StreamPair):
    #     """
    #     Handles the get files request, and sends files to given socket
    #     """
    #     self._peers[peer].log.debug("getting all files in cluster")
    #     await self._requests.join() # wait for no more file updates
    #     await Message.write(peer.writer, self.get_files())


    # async def _receive_update(self, peer: StreamPair, **update_args: Any):
    #     """ Notify update handler of a update task, and wait for completion """
    #     self._peers[peer].log.debug("updating file info")
    #     signal = self._peers[peer].signal
    #     signal.clear()
    #     await self._requests.put((Indexer.UPDATE_PRIORITY, RequestCall(peer, update_args)))
    #     await signal.wait() # block stream access til finished


    # async def _query_file(self, peer: StreamPair, filename: str):
    #     """ Reply to stream with the peers that have specified file """
    #     self._peers[peer].log.debug(f'querying for file {filename}')
    #     await self._requests.join() # wait for no more file updates
    #     await Message.write(peer.writer, self.get_location(filename))

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

    indexer = SuperPeer(**args)
    aio.run(indexer.start_server())

