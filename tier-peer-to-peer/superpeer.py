#!/usr/bin/env python3

from __future__ import annotations

import asyncio as aio
import json
import logging
import socket

from dataclasses import dataclass, field
from typing import Any, Optional, Union

from argparse import ArgumentParser
from pathlib import Path
from time import time

from lib.topology import Graph
from lib.connection import (
    Location, Login, Message, Procedure, Query, Request, StreamPair,
    ainput, getpeerbystream, merge_config_args, version_check
)


@dataclass(frozen=True)
class WeakState:
    """ Weak peer connection state """
    location: Location
    sender: StreamPair
    files: set[str] = field(default_factory=set)
    log: logging.Logger = field(default_factory=logging.getLogger)


@dataclass(init=False)
class SuperState:
    """ Super peer connection state """
    _location: Location
    _neighbors: list[int]

    receiver: Optional[StreamPair]
    sender: Optional[StreamPair]
    log: logging.Logger

    def __init__(self, host: Optional[str], port: int, neighbors: Optional[list[int]]):
        if not host:
            host = socket.gethostname()

        if not neighbors:
            neighbors = list()

        self._location = Location(host, port)
        self._neighbors = neighbors

        self.receiver = None
        self.sender = None
        self.log = default_logger(logging.getLogger())

    @property 
    def location(self):
        return self._location

    @property
    def neighbors(self):
        return self._neighbors


class RequestCall:
    """ Pending request to a given connection """
    _conn: StreamPair
    _args: dict[str, Any]

    def __init__(self, request: Request, conn: StreamPair, **kwargs: Any):
        self._conn = conn
        self._args = dict(req_type=request, **kwargs)

    @property
    def conn(self):
        return self._conn

    def __call__(self):
        """ Sends the request to the connection stream """
        return self._conn.request(**self._args)


class SuperPeer:
    """ Super peer node """
    _port: int
    _queries: dict[str, StreamPair]

    _supers: dict[str, SuperState]
    _weaks: dict[str, WeakState]
    _min_supers: Graph[str]
    _remote_files: dict[StreamPair, frozenset[str]]

    # async state, not defined in init
    _requests: aio.Queue[RequestCall]

    PROMPT = (
        "1. List active weak peers\n"
        "2. List active strong peers\n"
        "3. List all files in the system\n"
        "4. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")


    def __init__(self, port: int, supers: list[SuperState], **_):
        """
        Create the state structures for a strong peer node; to start running 
        the server, run the function start_server
        """
        self._port = max(1, port)

        super_ids = [SuperPeer.id(state) for state in supers]
        for id, state in zip(super_ids, supers):
            state.log = default_logger(logging.getLogger(id))

        self_id = SuperPeer.id(self)
        self_state = [
            state
            for id, state in zip(super_ids, supers)
            if id == self_id ][0]

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
        self._remote_files = dict()


    @staticmethod
    def id(super: Union[SuperPeer, SuperState]) -> str:
        """ Unique identifier for a super peer """
        if isinstance(super, SuperPeer):
            return f'{socket.gethostname()}:{super._port}'
        else:
            return f'{super.location[0]}:{super.location[1]}'


    #region getters

    def get_files(self) -> frozenset[str]:
        """ Gets all the unique files accross all peers """
        locals = frozenset(
            file
            for state in self._weaks.values()
            for file in state.files
        )

        remotes = frozenset(
            file
            for remote in self._remote_files.values()
            for file in remote
        )

        return locals | remotes


    def get_location(
        self, filename: str) -> set[Location]:
        """ Find the peer nodes associated with a file """
        locs = {
            state.location
            for state in self._weaks.values()
            if filename in state.files
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
                and st.sender != src) ]


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
            loc = sup.location
            connected = False
            fails, max_fails = 0, 5

            while not connected and fails < max_fails:
                try:
                    pair = await aio.open_connection(loc.host, loc.port)
                    pair = StreamPair(*pair)

                    login = Login(SuperPeer.id(self), host, self._port)
                    await Message.write(pair.writer, login)

                    sup.sender = pair
                    connected = True

                except ConnectionRefusedError:
                    fails += 1
                    await aio.sleep(5) # TODO: make param

        await aio.gather(*[connect_task(sup) for sup in self._supers.values()])



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

                logging.info('waiting for requests')
                req_call = await self._requests.get()
                logging.info('got a request')
                requests.add(
                    aio.create_task(request_task(req_call))
                )

        except aio.CancelledError:
            for req in requests: req.cancel()



    async def _session(self):
        """ Cli for indexer """
        while option := await ainput(SuperPeer.PROMPT):
            if option == '1':
                weak_peers = '\n'.join(
                    str(s.location)
                    for s in self._weaks.values()
                    if not s.sender.writer.is_closing()
                )
                print(f'The weak peers connected are:\n{weak_peers}\n')

            elif option == '2':
                super_peers = '\n'.join(
                    str(s.location)
                    for s in self._supers.values()
                    if s.sender and not s.sender.writer.is_closing()
                )
                print(f'The super peers connected are: \n{super_peers}\n')

            elif option == '3':
                files = '\n'.join(self.get_files())
                print('The files on the system are:')
                print(f'{files}\n')

            else:
                break

        print('Exiting server')


    #region receivers

    async def _super_receiver(self, login: Login, conn: StreamPair):
        """ Server-side connection with another strong peer """
        reader, writer = conn
        logger = self._supers[login.id].log

        async def query(query: Query):
            """ Actions for query requests from super peers """
            if query.is_hit:
                await query_hit(query)
            else:
                await query_poll(query)


        async def query_hit(hit: Query):
            """ Actions for query hit requests from super peers """
            if hit.id not in self._queries:
                logging.error(f'hit for {hit.id} does not exist')
                return

            src = self._queries[hit.id]
            await self._requests.put(
                RequestCall(Request.QUERY, src, query=hit))


        async def query_poll(poll: Query):
            """ Actions for polling query requests from super peers """
            if poll.id in self._queries:
                logging.error(f'got a duplicate query')
                return

            if poll.alive_time <= 0:
                logging.info(f'query {poll.id} has timed out')
                return

            # potential issue with readding an old query
            start = time()
            self._queries[poll.id] = self.get_sender(login)
            await self._local_query(poll)

            elapsed = time() - start
            new_query = poll.elapsed(elapsed)
            await self._forward_query(new_query)


        async def update(files: frozenset[str]):
            """ Actions for update requests from super peers """
            if self._remote_files[conn] == files:
                logging.error(f"received an unnecessary update")
                return

            sender = self.get_sender(login)
            self._remote_files[conn] = files
            await self._forward_update(sender, files)

        try:
            writer.write_eof() # do not use this stream for writing
            self._supers[login.id].receiver = conn

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
            """ Actions for query requests from weak peers """
            logger.info(f'query for {filename}')
            conn_info = getpeerbystream(conn)
            curr_time = time()

            # not checks for multiple queries of the same file
            id = f'{conn_info[0]}:{conn_info[1]}:{filename}:{curr_time}'
            timeout = 30 # TODO: make param
            weak_query = Query(id, filename, timeout)

            self._queries[weak_query.id] = conn
            await self._local_query(weak_query)
            await self._forward_query(weak_query)


        async def update(files: frozenset[str]):
            """ Actions for update requests from weak peers """
            weak = self._weaks[login.id]
            if weak.files == files:
                logging.error(f"received an unnecessary update")
                return

            logger.info(f'updating files')
            weak.files.clear()
            weak.files.update(files)

            await self._forward_update(conn, files)
            logging.info('finished updating')


        async def files():
            """ Actions for files requests from weak peers """
            logger.info(f'request for file list')
            files = self.get_files()
            
            await self._requests.put(
                RequestCall(Request.FILES, conn, files=files))

        try:
            self._weaks[login.id] = WeakState(login.location, conn, log=logger)
            logger.info(f'connected to {login}')

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
            hit = Query(query.id, query.filename, _locations=locs)
            await self._requests.put(
                RequestCall(Request.QUERY, src, query=hit))

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
        targets = self.get_forward_targets(src)
        if not targets: return

        await aio.gather(*[
            query_request(sender) for sender in targets
        ])


    async def _forward_update(self, src: StreamPair, files: frozenset[str]):
        """
        Passes along a received update to all neighboring supers, if they are
        in the min span
        """
        def update_request(sender: StreamPair):
            logging.info(f'forwarding update to {getpeerbystream(sender)}')
            return self._requests.put(
                RequestCall(Request.UPDATE, sender, files=files))

        targets = self.get_forward_targets(src)
        if not targets: return

        await aio.gather(*[
            update_request(sender) for sender in targets
        ])

    #endregion



def parse_json(map: Union[Path, str], **_) -> list[SuperState]:
    """
    Read a json file as starting super states; the expected format
    is as an array of objects, where each object has a host, port,
    and neighbors attribute
    """
    if isinstance(map, str):
        map = Path(map)

    map = map.with_suffix('.json')
    with open(map) as f:
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
    args.add_argument("-l", "--log", default='super.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")
    args.add_argument("-m", "--map", help="the super peer structure json file map")

    args = args.parse_args()

    args = merge_config_args(args)

    init_log(**args)

    supers = parse_json(**args)
    indexer = SuperPeer(supers=supers, **args)
    aio.run(indexer.start_server())

