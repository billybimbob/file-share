#!/usr/bin/env python3

from __future__ import annotations

import asyncio as aio
import hashlib
import logging
import re
import socket

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Optional, Union, cast

from argparse import ArgumentParser
from functools import partial
from pathlib import Path
from time import time

from lib.connection import (
    CHUNK_SIZE,
    File, Location, Login, Message, Procedure,
    Response, Request, StreamPair,
    ainput, merge_config_args, version_check
)



@dataclass(frozen=True)
class SuperConnection:
    """ All information about the super server connection """
    location: Location
    stream: StreamPair
    access: aio.Lock = field(default_factory=aio.Lock)


class RequestsHandler:
    """
    Synchronized handler for all received procedures from the super peer 
    """

    @dataclass
    class Waiter:
        """ Waiting stat for a request """
        response: Optional[Union[Response, frozenset[str]]] = None
        num_waiters: int = 0


    _conn: Optional[SuperConnection]
    _got: aio.Condition
    _queries: defaultdict[str, Waiter]
    _files: Waiter

    def __init__(self):
        self._conn = None
        self._queries = defaultdict(RequestsHandler.Waiter)
        self._files = RequestsHandler.Waiter()


    async def listener(self, conn: SuperConnection):
        """
        Listener that switches the receive action based on the 
        request type, that runs until cancelled
        """
        self._conn = conn
        try:
            reader = self._conn.stream.reader
            while True:
                procedure = await Message[Procedure].read(reader)
                request = procedure.request

                if request is Request.FILES:
                    await procedure(self._got_files)
                elif request is Request.QUERY:
                    await procedure(self._got_hit)
                else:
                    logging.error(f'got an unknown {request=}')

        except aio.CancelledError:
            pass


    async def _got_files(self, files: frozenset[str]):
        """ Actions for file request responses """
        async with self._got:
            self._files.response = files
            self._got.notify_all()

            # wait for should release the lock
            await self._got.wait_for(lambda: self._files.num_waiters == 0)

            self._files.response = None


    async def _got_hit(self, response: Response):
        """ Actions for query request responses """
        query = self._queries[response.file.name]
        async with self._got:
            query.response = response
            self._got.notify_all()

            await self._got.wait_for(lambda: query.num_waiters == 0)
            query.response = None


    async def wait_for_files(self) -> frozenset[str]:
        """
        Sends and then waits for a file request response; simultaneous 
        file requests converge to the same request-and-response cycle
        """
        if not self._conn:
            raise aio.InvalidStateError('Super connection not initialized')

        filewait = self._files
        async with self._got:
            filewait.num_waiters += 1

            # make sure only one active request
            if self._files.num_waiters == 1:
                async with self._conn.access:
                    await self._conn.stream.request(Request.FILES)
                    logging.info('sent request for files')

            await self._got.wait_for(lambda: filewait.response is not None) 

            # guaranteed not be None and a frozenset
            files = cast(frozenset[str], filewait.response)
            filewait.num_waiters -= 1
            self._got.notify_all()

            return files


    async def wait_for_query(self, filename: str) -> Response:
        """
        Sends and then waits for a query request response for the given file;
        a new request is only sent to the super peer if new_req is True
        """
        if not self._conn:
            raise aio.InvalidStateError('Super connection not initialized')

        waiter = self._queries[filename]
        async with self._got:
            waiter.num_waiters += 1

            if waiter.num_waiters == 1:
                async with self._conn.access:
                    await self._conn.stream.request(
                        Request.QUERY, filename=filename)
                    logging.info(f'sent request for {filename}')

            await self._got.wait_for(lambda: waiter.response is not None)                    

            response = cast(Response, waiter.response)
            logging.info(f'got result for {filename}')

            waiter.num_waiters -= 1
            self._got.notify_all()

            return response



class WeakPeer:
    """ Represents a single peer node """
    _port: int
    _user: str
    _direct: Path
    _super_start: Location

    # async state, not defined in init
    _requests: RequestsHandler
    _dir_update: aio.Event

    PROMPT = (
        "1. List all available files in system\n"
        "2. Download a file\n"
        "3. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")


    def __init__(
        self,
        port: int,
        sup_port: int,
        user: Optional[str] = None,
        address: Optional[str] = None,
        directory: Union[str, Path, None] = None,
        **_):
        """
        Creates all the state information for a peer node, use to method
        start_server to activate the peer node
        """
        self._port = max(1, port)
        self._user = socket.gethostname() if user is None else user

        if directory is None:
            self._direct = Path('.')
        elif isinstance(directory, str):
            self._direct = Path(f'./{directory}')
        else:
            self._direct = directory

        self._direct.mkdir(exist_ok=True, parents=True)

        if address is None:
            in_host = socket.gethostname()
        else:
            in_host = socket.gethostbyaddr(address)[0]

        self._super_start = Location(in_host, sup_port)


    def get_files(self) -> frozenset[File.Data]:
        """ Gets all the files from the direct path attribute """
        return frozenset(
            File.Data(p.name, p.stat().st_size)
            for p in self._direct.iterdir()
            if p.is_file()
        )


    async def start_server(self):
        """
        Connects the peer to the indexing node, and starts the peer server
        """
        # async sync state needs to be init from async context
        self._dir_update = aio.Event()
        self._requests = RequestsHandler()

        def to_upload(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Weak peer closure as a stream callback """
            return self._peer_upload(StreamPair(reader, writer))

        try:
            host = socket.gethostname()
            server = await aio.start_server(
                to_upload, host, self._port, start_serving=False)
            logging.info(f'peer server on {host}, port {self._port}')

            async with server:
                if not server.sockets:
                    return

                super_peer = await self._connect_super(host)

                # make sure that event is listening before it can be set and cleared
                first_update = aio.create_task(self._dir_update.wait())

                sender = aio.create_task(self._update_dir(super_peer))
                listener = aio.create_task(self._requests.listener(super_peer))

                # only accept connections and user input after a dir_update
                # should not deadlock, keep eye on
                await first_update
                await server.start_serving()

                session = aio.create_task(self._session())
                watch = aio.create_task(self._watch_connection(super_peer))

                await aio.wait([session, watch], return_when=aio.FIRST_COMPLETED)

                sender.cancel()
                listener.cancel()

                if not session.done(): session.cancel()
                if not watch.done(): watch.cancel()

            await server.wait_closed()

        except Exception as e:
            logging.exception(e)
            for task in aio.all_tasks():
                task.cancel()

        finally:
            logging.debug("disconnected from indexing server")
            logging.debug("ending peer")



    async def _connect_super(self, host: str) -> SuperConnection:
        """ Establishes a connection with the super peer """
        start = self._super_start

        logging.info(f'trying connection with super at {start}')

        in_pair = await aio.open_connection(start.host, start.port)
        in_pair = StreamPair(*in_pair)
        logging.info(f'connection with super')

        login = Login(self._user, host, self._port)
        await Message.write(in_pair.writer, login)

        return SuperConnection(start, in_pair)



    async def _update_dir(self, conn: SuperConnection):
        """
        Sends updated directory to indexer, should be the only place where
        files attr is updated in order to sync with indexer
        """
        async def check_dir():
            """
            Polls on directory file name changes, and raises event updates
            on change found
            """
            try:
                last_check = None
                while True:
                    check = self.get_files()
                    if last_check != check:
                        last_check = check
                        self._dir_update.set()

                    await aio.sleep(5) # TODO: make interval param

            except aio.CancelledError:
                pass

        checker = aio.create_task(check_dir())
        try:
            while True:
                await self._dir_update.wait()

                async with conn.access:
                    files = self.get_files()
                    await conn.stream.request(Request.UPDATE, files=files)
                    # should be only place where dir_update is cleared
                    self._dir_update.clear()

        except aio.CancelledError:
            checker.cancel()



    async def _watch_connection(self, conn: SuperConnection):
        """ Coroutine that completes when the indexer connection is closed """
        reader, writer = conn.stream
        try:
            while not reader.at_eof():
                await aio.sleep(8) # TODO: make interval param

            logging.info("Indexer connection has ended")
            print('\nConnection to indexer was closed, press enter to continue')

        except aio.CancelledError:
            pass

        finally:
            writer.close()
            await writer.wait_closed()


    #region session methods

    async def _session(self):
        """ Cli for peer """
        try:
            cmd_count = 0
            while request := await ainput(WeakPeer.PROMPT):
                logging.info(f'request # {cmd_count}')
                cmd_count += 1

                if request == '1':
                    await self._list_system()

                elif request == '2':
                    await self._run_download()

                else:
                    print(f'Exiting {self._user} peer server')
                    break

        except aio.CancelledError:
            pass

        except Exception as e:
            logging.exception(e)

        finally:
            logging.info(f"{self._user} session ending")



    async def _system_files(self) -> list[str]:
        """ Fetches all the files in the system based on indexer """
        logging.debug('get system files')
        files = await self._requests.wait_for_files()
        return sorted(files)


    async def _list_system(self):
        """ Fetches then prints the system files """
        files = await self._system_files()
        print('The files on the system:')
        print('\n'.join(files))



    async def _run_download(self):
        """
        Queries the indexer, prompts the user, then fetches file from peer
        """
        logging.debug('getting download options')

        start_time = time()
        files = await self._system_files()
        elapsed = time() - start_time

        logging.info(f"response time for getting files took {elapsed:.4f} secs")

        if len(files) == 0:
            print('There are no files that can be downloaded')
            return

        picked = await self._select_file(files)

        if picked is None:
            print('invalid file entered')
            return

        start_time = time()
        response = await self._requests.wait_for_query(picked)
        elapsed = time() - start_time
        logging.info(f"query {picked} took {elapsed:.4f} secs")

        peers = set(response.locations)

        self_loc = Location(socket.gethostname(), self._port)
        target = None

        while all([not target, peers]):
            # use a random peer target
            peer = peers.pop()
            if peer != self_loc:
                target = peer


        if target is None: # must have timed out
            logging.error(f"location for {picked} cannot be found")
            return

        # TODO: split file into chunks
        await self._peer_download(picked, target, response.file.size)
        self._dir_update.set()


    async def _select_file(self, fileset: Iterable[str]) -> Optional[str]:
        """ Take in user input to select a file from the given set """
        # sort might be slow
        files = sorted(fileset)
        options = (f'{i+1}: {file}' for i, file in enumerate(files))

        print('\n'.join(options))
        choice = await ainput("enter file to download: ")

        logging.info(f'{len(files)} options, {choice=}')

        if choice.isnumeric():
            idx = int(choice)-1
            if idx >= 0 and idx < len(files):
                choice = files[idx]

        if choice not in fileset:
            return None

        return choice


    async def _peer_download(self, target: Location, *chunks: File.Chunk):
        """ Client-side connection with any of the other peers """
        logging.debug(f"attempting connection to {target}")

        pair = await aio.open_connection(target.host, target.port)
        pair = StreamPair(*pair)
        reader, writer = pair

        log = logging.getLogger()

        try:
            await Message.write(writer, self._user)
            user = await Message[str].read(reader)

            log = logging.getLogger(user)
            log.debug("connected")

            await self._receive_file_retries(pair, log, *chunks)

        except Exception as e:
            logging.exception(e)

        finally:
            if not reader.at_eof():
                writer.write_eof()

            writer.close()
            await writer.wait_closed()
            log.debug("disconnected")



    async def _receive_file_retries(
        self, peer: StreamPair, log: logging.Logger, *chunks: File.Chunk):
        """
        Runs multiple attempts to download a file from the server if needed
        """
        filename = chunks[0].name
        if not all(c.name == filename for c in chunks):
            raise ValueError('all chunk names must be the same')

        start_time = time()

        filepath = self._direct.joinpath(filename)
        exts = "".join(filepath.suffixes)
        stem = filepath.stem

        # can have issues with different files having the same name
        if m := re.match(r'\)(\d+)\(', stem[::-1]):
            # use dup modifier if it exists
            dup_mod = int(m[1])
            stem = stem[:-m.end()]
        else:
            dup_mod = 1

        while filepath.exists():
            filepath = filepath.with_name(f'{stem}({dup_mod}){exts}')
            dup_mod += 1

        got_file = False
        num_tries = 0
        tot_read = 0

        while not got_file and num_tries < 5: # TODO: make param
            got_file, amt_read = await peer.request(
                Request.DOWNLOAD,
                filename=filename,
                receiver=partial(self._receive_file, filepath)
            )
            # await self._receive_file(filepath, reader)
            num_tries += 1
            tot_read += amt_read

        elapsed = time() - start_time
        log.info(f"successfully got file")
        log.debug(f'received {filename} was {elapsed:.4f} secs')
        log.info(f'read {(tot_read / 1000):.2f} KB')



    async def _receive_file(self, filepath: Path, reader: aio.StreamReader) -> tuple[bool, int]:
        """ Used by the client side to download and verify correctness of download """
        checksum_passed = False
        amt_read = 0

        # expect the checksum to be sent first
        checksum = await Message[bytes].read(reader)

        with open(filepath, 'w+b') as f:
            filesize = await Message[int].read(reader)

            while amt_read < filesize:
                # no messages since each file chunk is part of same "message"
                chunk = await reader.read(CHUNK_SIZE)
                f.write(chunk)
                amt_read += len(chunk)

            f.seek(0)
            local_checksum = hashlib.md5()
            for line in f:
                local_checksum.update(line)

            local_checksum = local_checksum.digest()
            checksum_passed = local_checksum == checksum

        return checksum_passed, amt_read

    #endregion


    async def _peer_upload(self, peer: StreamPair):
        """ Server-side connection with any of the other peers """
        reader, writer = peer
        log = logging.getLogger()

        try:
            user = await Message[str].read(reader)
            await Message.write(writer, self._user)

            log = logging.getLogger(user)
            log.debug(f"connected")

            while procedure := await Message[Procedure].read(reader):
                if procedure.request is Request.DOWNLOAD:
                    await procedure(self._send_file, peer, log=log)
                else:
                    raise RuntimeError("Request type is not possible")

        except aio.IncompleteReadError:
            pass

        except Exception as e:
            log.exception(e)
            await Message.write(writer, e)

        finally:
            if not reader.at_eof():
                writer.write_eof()

            writer.close()
            await writer.wait_closed()
            log.debug(f"disconnected")


    async def _send_file(
        self, peer: StreamPair, filename: str, log: logging.Logger):
        """
        Runs an attempts to send a file to a peer
        """
        start_time = time()
        _, writer = peer
        filepath = self._direct.joinpath(filename)

        filesize = filepath.stat().st_size

        with open(filepath, 'rb') as f:
            checksum = hashlib.md5()
            for line in f:
                checksum.update(line)
            checksum = checksum.digest()

            # logger.info(f'checksum of: {checksum}')
            await Message.write(writer, checksum)
            await Message.write(writer, filesize)

            f.seek(0)
            writer.writelines(f) # don't need to encode
            await writer.drain()

        elapsed = time() - start_time
        log.debug(
            f'sent {filename}: {(filesize/1000):.2f} KB in {elapsed:.5f} secs'
        )



def init_log(log: str, verbosity: int, **_):
    """ Specifies logging format and location """
    log_path = Path(f'./{log}').with_suffix('.log')
    log_path.parent.mkdir(exist_ok=True, parents=True)
    log_settings = {
        'format': "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s: %(message)s",
        'datefmt': "%H:%M:%S",
        'level': logging.getLevelName(verbosity)
    }

    if not log_path.exists() or log_path.is_file():
        logging.basicConfig(filename=log, filemode='w', **log_settings)
    else: # just use stdout
        logging.basicConfig(**log_settings)


if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser(description="creates a peer node")
    args.add_argument("-a", "--address", default=None, help="ip address of the indexing server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--directory", default='', help="the client download folder")
    args.add_argument("-l", "--log", default='weak.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=9989, help="the port to listen for connections")
    args.add_argument("-s", "--sup-port", type=int, default=8889, help="the port of the super server")
    args.add_argument("-u", "--user", help="username of the client connecting")
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")

    args = args.parse_args()
    args = merge_config_args(args)

    init_log(**args)

    peer = WeakPeer(**args)
    aio.run(peer.start_server())
