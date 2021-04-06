#!/usr/bin/env python3

from __future__ import annotations

import asyncio as aio
import logging
import socket

from dataclasses import dataclass, field
from typing import Any

from argparse import ArgumentParser
from pathlib import Path

from lib.connection import (
    File, Location, Login, Message, Procedure, Response, Request, StreamPair,
    ainput, getpeerbystream, merge_config_args, version_check
)


@dataclass(frozen=True)
class PeerState:
    """ Weak peer connection state """
    location: Location
    sender: StreamPair
    files: set[File.Data] = field(default_factory=set)
    log: logging.Logger = field(default_factory=logging.getLogger)


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


class Indexer:
    """ Super peer node """
    _port: int
    _peers: dict[str, PeerState]

    # async state, not defined in init
    _requests: aio.Queue[RequestCall]

    PROMPT = (
        "1. List active weak peers\n"
        "2. List all files in the system\n"
        "3. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")


    def __init__(self, port: int, **_):
        """
        Create the state structures for a strong peer node; to start running 
        the server, run the function start_server
        """
        self._port = max(1, port)
        self._peers = dict()


    #region getters

    def get_files(self) -> frozenset[File.Data]:
        """ Gets all the unique files accross all peers """
        files = frozenset(
            file
            for state in self._peers.values()
            for file in state.files
        )

        return files


    def get_location(self, file: File.Data) -> frozenset[Location]:
        """ Find the peer nodes associated with a file """
        locs = frozenset(
            peer.location
            for peer in self._peers.values()
            if file in peer.files)

        return locs

    #endregion


    async def start_server(self):
        """
        Initializes the indexer server and workers; awaiting on
        this method will wait until the server is closed
        """
        # async sync state needs to be init from async context
        self._requests = aio.Queue()

        async def to_receiver(
            reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Switch to determine which receiver to use """
            conn = StreamPair(reader, writer)
            login = await Message[Login].read(reader) # TODO: add try except

            await self._weak_receiver(login, conn)

        try:
            host = socket.gethostname()
            server = await aio.start_server(
                to_receiver, host, self._port, start_serving=False)

            async with server:
                if not server.sockets:
                    return

                addr = server.sockets[0].getsockname()
                logging.info(f'super server on {addr[:2]}')

                caller = aio.create_task(self._request_caller())

                await server.start_serving()
                # make sure to serve before connects, else will be deadlocks
                await aio.create_task(self._session())

                caller.cancel()

            await server.wait_closed()

        except Exception as e:
            logging.exception(e)
            for task in aio.all_tasks():
                task.cancel()

        finally:
            logging.info("super server stopped")


    async def _request_caller(self):
        """ Handles sending all requests to specified targets"""
        used_conns = set[StreamPair]()
        usage_check = aio.Condition()

        async def request_task(req_call: RequestCall):
            """
            Execute requests while making sure concurrent stream access does
            not occur
            """
            conn = req_call.conn
            try:
                async with usage_check:
                    await usage_check.wait_for(
                        lambda: conn not in used_conns)
                    used_conns.add(conn)

                await req_call()
                async with usage_check:
                    used_conns.remove(conn)
                    usage_check.notify_all()

                self._requests.task_done()

            except aio.CancelledError:
                pass

            except Exception as e:
                logging.exception(f'{getpeerbystream(conn)}: {e}')

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



    async def _session(self):
        """ Cli for indexer """
        while option := await ainput(Indexer.PROMPT):
            if option == '1':
                weak_peers = '\n'.join(
                    str(s.location)
                    for s in self._peers.values()
                    if not s.sender.writer.is_closing()
                )
                print(f'The weak peers connected are:\n{weak_peers}\n')

            elif option == '2':
                files = '\n'.join(f.name for f in self.get_files())
                print('The files on the system are:')
                print(f'{files}\n')

            else:
                break

        print('Exiting server')


    #region receivers

    async def _weak_receiver(self, login: Login, conn: StreamPair):
        """ Server-side connection with a weak peer """
        reader, writer = conn
        logger = default_logger(logging.getLogger(login.id))

        async def query(file: File.Data):
            """ Actions for query requests from weak peers """
            logger.info(f'query for {file.name}')

            locs = self.get_location(file)
            response = Response(file, locs)

            await self._requests.put(
                RequestCall(Request.QUERY, conn, response=response))


        async def update(files: frozenset[File.Data]):
            """ Actions for update requests from weak peers """
            peer = self._peers[login.id]
            if peer.files == files and files:
                logger.error(f"received an unnecessary update")
                return

            logger.info('updating files')
            peer.files.clear()
            peer.files.update(files)


        async def files():
            """ Actions for files requests from weak peers """
            logger.info('request for file list')
            files = self.get_files()
            
            await self._requests.put(
                RequestCall(Request.FILES, conn, files=files))

        try:
            self._peers[login.id] = PeerState(login.location, conn, log=logger)
            logger.info(f'connected')

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

            # update weak files, weak files invalidated
            del self._peers[login.id]
            await writer.wait_closed() # delete super state


    #endregion



def init_log(log: str, **_):
    """ Specifies logging format and location """
    log_path = Path(f'./{log}').with_suffix('.log')
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

    indexer = Indexer(**args)
    aio.run(indexer.start_server())

