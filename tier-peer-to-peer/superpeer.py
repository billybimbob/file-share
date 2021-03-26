#!/usr/bin/env python3

from __future__ import annotations

import asyncio as aio
import json
import logging
import socket

from dataclasses import dataclass, field
from typing import Any, Optional, TypedDict, Union

from argparse import ArgumentParser
from pathlib import Path
from time import time

from topology import Graph
from connection import (
    Location, Login, Message, Procedure, Query, QueryHit,
    Request, StreamPair,
    ainput, getpeerbystream, merge_config_args, version_check
)


@dataclass(frozen=True)
class WeakState:
    """ Weak peer connection state """
    loc: Location
    sender: StreamPair
    files: set[str] = field(default_factory=set)
    log: logging.Logger = field(default_factory=logging.getLogger)


@dataclass(init=False)
class SuperState:
    """ Strong peer connection state """
    loc: Location
    neighbors: list[int]
    receiver: Optional[StreamPair]
    sender: Optional[StreamPair]
    log: logging.Logger

    def __init__(self, host: str, port: int, neighbors: list[int]):
        self.loc = Location(host, port)
        self.neighbors = neighbors

        self.receiver = None
        self.sender = None

        self.log = default_logger(logging.getLogger())


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

    @property
    def conn(self):
        return self._conn

    def __call__(self):
        return self._conn.request(**self._args)


class SuperPeer:
    """ Super peer node """
    _port: int
    _queries: dict[str, StreamPair]
    _requests: aio.Queue[RequestCall]

    _supers: dict[str, SuperState]
    _min_supers: Graph[str]

    _weaks: dict[str, WeakState]
    _remote_files: dict[StreamPair, frozenset[str]]

    PROMPT = (
        "1. List active weak peers\n"
        "2. List active strong peers\n"
        "3. List all files in the system\n"
        "4. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")


    def __init__(self, port: int, supers: list[SuperState]):
        """
        Create the state structures for a strong peer node; to start running the server,
        run the function start_server
        """
        self._port = max(1, port)

        super_ids = [SuperPeer.id(state) for state in supers]
        for id, state in zip(super_ids, supers):
            state.log = default_logger(logging.getLogger(id))

        self_id = SuperPeer.id(self)
        self_state = [
            state
            for id, state in zip(super_ids, supers)
            if id == self_id
        ].pop()

        self._queries = dict()
        self._weaks = dict()
        self._supers = {
            super_ids[n]: supers[n]
            for n in self_state.neighbors
        }

        super_map = {
            id: [super_ids[n] for n in state.neighbors]
            for id, state in zip(super_ids, supers)
        }

        self._min_supers = Graph.from_map(super_map).min_span()


    @staticmethod
    def id(super: Union[SuperPeer, SuperState]) -> str:
        """ Unique identifier for a super peer """
        if isinstance(super, SuperPeer):
            return f'{socket.gethostname()}:{super._port}'
        else:
            return f'{super.loc[0]}:{super.loc[1]}'


    #region getters

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


    def get_location(self, filename: str) -> set[Location]:
        """ Find the peer nodes associated with a file """
        locs = {
            info.loc
            for info in self._weaks.values()
            if filename in info.files
        }

        return locs


    def get_forward_targets(self, src: StreamPair) -> list[StreamPair]:
        """
        Gets all neighboring super peer connections excluding the
        source neighbor and ones not in the min span
        """
        self_id = SuperPeer.id(self)
        return [
            st.sender
            for id, st in self._supers.items()
            if (st.sender # should not be none at this point
                and self._min_supers.has_connection(self_id, id)
                and st.sender != src)
        ]


    def get_sender(self, login: Login) -> StreamPair:
        """ Gets the sender assciated with the super login """
        state = self._supers[login.id]
        if state.sender is None:
            raise aio.InvalidStateError('sender is not init')

        return state.sender

    #endregion


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
                # make sure to serve before connects, else will be deadlocks
                await self._connect_supers(host)
                await self._session()

                caller.cancel()

            await server.wait_closed()

        except Exception as e:
            logging.exception(e)
            for task in aio.all_tasks():
                task.cancel()

        finally:
            logging.info("index server stopped")


    async def _connect_supers(self, host: str):
        """ Connects to all specified super peers """
        async def connect_task(sup: SuperState):
            loc = sup.loc
            conn = aio.create_task(aio.open_connection(loc.host, loc.port))
            fin, pend = await aio.wait({conn}, timeout=8)

            for p in pend: p.cancel()
            if len(fin) == 0:
                raise aio.TimeoutError("Connection could not be established")

            pair = fin.pop().result()
            pair = StreamPair(*pair)

            login = Login(SuperPeer.id(self), host, self._port)
            await Message.write(pair.writer, login)

            sup.sender = pair

        await aio.gather(*[connect_task(sup) for sup in self._supers.values()])


    async def _session(self):
        """ Cli for indexer """
        while option := await ainput(SuperPeer.PROMPT):
            if option == '1':
                peer_users = '\n'.join(
                    str( getpeerbystream(s.sender) )
                    for s in self._weaks.values()
                    if not s.sender.writer.is_closing()
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
        used_conns = set[StreamPair]()
        usage_check = aio.Condition()

        async def request_task(req_call: RequestCall):
            """
            Execute requests while making sure concurrent stream access does
            not occur
            """
            try:
                conn = req_call.conn
                async with usage_check:
                    await usage_check.wait_for(
                        lambda: conn not in used_conns
                    )
                    used_conns.add(conn)

                await req_call()

                async with usage_check:
                    used_conns.remove(conn)
                    usage_check.notify_all()

                self._requests.task_done()

            except aio.CancelledError:
                pass

        requests = set[aio.Task[None]]()

        try:
            while True:
                completed = { req for req in requests if req.done() }
                # might be expensive
                if completed:
                    requests -= completed

                req_call = await self._requests.get()
                requests.add(
                    aio.create_task(request_task(req_call))
                )

        except aio.CancelledError:
            for req in requests: req.cancel()


    #region receivers

    async def _super_receiver(self, login: Login, conn: StreamPair):
        """ Server-side connection with another strong peer """
        reader, writer = conn
        logger = self._supers[login.id].log

        async def query(query: Query):
            """ Actions for strong query requests """
            start = time()
            self._queries[query.id] = self.get_sender(login)
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
            sender = self.get_sender(login)
            self._remote_files[conn] = files
            await self._forward_update(sender, files)

        try:
            writer.write_eof() # do not use server for writing
            self._supers[login.id].receiver = conn

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

        finally:
            logger.debug('ending connection')
            writer.close()
            del self._supers[login.id]
            await writer.wait_closed() # delete super state



    async def _weak_receiver(self, login: Login, conn: StreamPair):
        """ Server-side connection with a weak peer """
        reader, writer = conn
        logger = default_logger(logging.getLogger(login.id))

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
            weak = self._weaks[login.id]
            weak.files.clear()
            weak.files.update(files)
            await self._forward_update(conn, files)


        async def files():
            """ Actions for weak files requests """
            await self._requests.put(
                RequestCall(Request.FILES, conn, files=self.get_files())
            )

        try:
            self._weaks[login.id] = WeakState(login.location, conn, log=logger)

            while procedure := await Message[Procedure].read(reader):
                request = procedure.request

                if request is Request.QUERY:
                    await procedure(query)
                elif request is Request.UPDATE:
                    await procedure(update)
                elif request is Request.FILES:
                    await procedure(files)
                else:
                    raise ValueError('Did not get an expected request')

        except aio.IncompleteReadError:
            pass

        except Exception as e:
            logger.exception(e)
            await Message.write(writer, e)

        finally:
            logger.debug('ending connection')
            writer.close()
            del self._weaks[login.id]
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
            hit = QueryHit(query.id, query.filename, locs)
            await self._requests.put(
                RequestCall(Request.HIT, src, hit=hit)
            )

        aio.create_task(alive_timer())


    async def _forward_query(self, query: Query):
        """
        Passes along a received query to all neighboring supers, if they are
        in the min span
        """
        def query_request(sender: StreamPair):
            return self._requests.put(
                RequestCall(Request.QUERY, sender, query=query))

        src = self._queries[query.id]
        await aio.gather(*[
            query_request(sender)
            for sender in self.get_forward_targets(src)
        ])


    async def _forward_update(self, src: StreamPair, files: frozenset[str]):
        """
        Passes along a received update to all neighboring supers, if they are
        in the min span
        """
        def update_request(sender: StreamPair):
            return self._requests.put(
                RequestCall(Request.UPDATE, sender, files=files))

        await aio.gather(*[
            update_request(sender)
            for sender in self.get_forward_targets(src)
        ])

    #endregion



def parse_json(path: Union[Path, str]) -> list[SuperState]:
    """
    Read a json file as starting super states; the expected format
    is as an array of objects, where each object has a host, port,
    and neighbors attribute
    """
    if isinstance(path, str):
        path = Path(path)

    path = path.with_suffix('.json')
    with open(path) as f:
        # potential type errors
        items: list[dict[str, Any]] = json.load(f)
        return [SuperState(**item) for item in items]


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

