#!/usr/bin/env python3

from __future__ import annotations

import asyncio as aio
import hashlib
import logging
import os
import re
import socket

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Optional, Union, cast

from argparse import ArgumentParser
from io import BufferedRandom
from pathlib import Path
from time import time

from connection import (
    File, Location, Login, Message, Procedure,
    Response, Request, StreamPair,
    ainput, merge_config_args, version_check)



@dataclass(frozen=True)
class IndexerConnection:
    """ All information about the indexer connection """
    location: Location
    stream: StreamPair
    access: aio.Lock = field(default_factory=aio.Lock)


class RequestsHandler:
    """
    Synchronized handler for all received procedures from the indexer 
    """
    @dataclass
    class Waiter:
        """ Waiting stat for a request """
        result: Union[Response, frozenset[File.Data], None] = None
        num_waiters: int = 0


    _conn: Optional[IndexerConnection]
    _got: aio.Condition
    _queries: defaultdict[str, Waiter]
    _files: Waiter

    def __init__(self):
        self._conn = None
        self._got = aio.Condition()
        self._queries = defaultdict(RequestsHandler.Waiter)
        self._files = RequestsHandler.Waiter()


    async def listener(self, conn: IndexerConnection):
        """
        Listener that switches the receive action based on the 
        request type, that runs until cancelled
        """
        self._conn = conn

        async def got_files(files: frozenset[File.Data]):
            """ Share file set to file waiters """
            await self._share_result(self._files, files)

        async def got_query(response: Response):
            """ Share query response to query waiters """
            await self._share_result(
                self._queries[response.file.name], response)

        try:
            reader = self._conn.stream.reader
            while True:
                procedure = await Message[Procedure].read(reader)
                request = procedure.request

                if request is Request.FILES:
                    await procedure(got_files)

                elif request is Request.QUERY:
                    await procedure(got_query)

                else:
                    logging.error(f'got an unknown {request=}')

        except aio.CancelledError:
            pass


    async def _share_result(
        self,
        waiter: Waiter,
        result: Union[Response, frozenset[File.Data]]):
        """ Send received response to the waiters """
        async with self._got:
            waiter.result = result
            self._got.notify_all()

            # wait for should release the lock
            await self._got.wait_for(lambda: waiter.num_waiters == 0)
            waiter.result = None


    async def _wait_for_result(
        self,
        waiter: Waiter,
        request: Request,
        *req_args: Any,
        **req_kwargs: Any) -> Union[frozenset[File.Data], Response]:
        """
        Waits for a result from the indexer from a previous query; 
        simultaneous file requests converge to the same request-and-response
        cycle
        """
        if not self._conn:
            raise aio.InvalidStateError('Indexer connection not initialized')

        async with self._got:
            waiter.num_waiters += 1

            if waiter.num_waiters == 1:
                async with self._conn.access:
                    await self._conn.stream.request(
                        request, *req_args, **req_kwargs)

            await self._got.wait_for(lambda: waiter.result is not None)

            # guaranteed to not be None
            result = cast(
                Union[frozenset[File.Data], Response], waiter.result)

            waiter.num_waiters -= 1
            self._got.notify_all()

            return result


    async def wait_for_files(self) -> frozenset[File.Data]:
        """
        Sends and then waits for a file request response; simultaneous 
        file requests converge to the same request-and-response cycle
        """
        logging.info('waiting for files')
        files = await self._wait_for_result(self._files, Request.FILES)

        return cast(frozenset[File.Data], files)


    async def wait_for_response(self, file: File.Data) -> Response:
        """
        Sends and then waits for a query request response for the given file
        """
        logging.info(f'waiting for {file.name}')
        response = await self._wait_for_result(
            self._queries[file.name], Request.QUERY, file=file)

        return cast(Response, response)



class Peer:
    """ Represents a single peer node """
    _port: int
    _user: str
    _direct: Path
    _index_start: Location

    # async state, not defined in init
    _requests: RequestsHandler
    _dir_update: aio.Event


    CHUNK_SIZE = 64_000 # 64KB

    PROMPT = (
        "1. List all available files in system\n"
        "2. Download a file\n"
        "3. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")


    def __init__(
        self,
        port: int,
        in_port: int,
        user: Optional[str] = None,
        address: Optional[str] = None,
        directory: Union[str, Path, None] = None,
        **_):
        """
        Creates all the state information for a peer node, use to method
        start_server to activate the peer node
        """
        self._port = max(1, port)
        self._user = (
            socket.gethostname() + str(self._port)
            if user is None else
            user)

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

        self._index_start = Location(in_host, in_port)


    def get_files(self) -> frozenset[File.Data]:
        """ Gets all the files from the direct path attribute """
        files = frozenset(
            File.Data(p.name, p.stat().st_size)
            for p in self._direct.iterdir()
            if p.is_file())
        
        return files


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

                indexer = await self._connect_index(host)

                # make sure that event is listening before it can be set and cleared
                first_update = aio.create_task(self._dir_update.wait())

                sender = aio.create_task(self._update_dir(indexer))
                listener = aio.create_task(self._requests.listener(indexer))

                # only accept connections and user input after a dir_update
                # should not deadlock, keep eye on
                await first_update
                await server.start_serving()

                session = aio.create_task(self._session())
                watch = aio.create_task(self._watch_connection(indexer))

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



    async def _connect_index(self, host: str) -> IndexerConnection:
        """ Establishes a connection with the indexer peer """
        start = self._index_start

        logging.info(f'trying connection with indexer at {start}')

        in_pair = await aio.open_connection(start.host, start.port)
        in_pair = StreamPair(*in_pair)
        logging.info(f'connection with indexer')

        login = Login(self._user, host, self._port)
        await Message.write(in_pair.writer, login)

        return IndexerConnection(start, in_pair)



    async def _update_dir(self, conn: IndexerConnection):
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
                    check = {
                        f.name
                        for f in self._direct.iterdir()
                        if f.is_file() }

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



    async def _watch_connection(self, conn: IndexerConnection):
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
            while request := await ainput(Peer.PROMPT):
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



    async def _system_files(self) -> list[File.Data]:
        """ Fetches all the files in the system based on indexer """
        logging.debug('get system files')
        files = await self._requests.wait_for_files()
        return sorted(files, key=lambda f: f.name)


    async def _list_system(self):
        """ Fetches then prints the system files """
        files = await self._system_files()
        print('The files on the system:')
        print('\n'.join(f.name for f in files))



    async def _run_download(self):
        """
        Queries the indexer, prompts the user, then fetches file from peer
        """
        try:
            picked = await self._run_selection()
            response = await self._get_response(picked)

            filepath = self._new_download_path(response)
            read_amt = response.file.size

            # make size the same as response
            with open(filepath, 'xb') as f:
                f.truncate(response.file.size)

            logging.info(f'downloading {picked.name} to {filepath.name}')
            start_time = time()

            try:
                await self._download_to_path(filepath, response)
                read_amt = filepath.stat().st_size

                print(f'Download for {picked.name} succeeded')
                self._dir_update.set()

            except Exception as e:
                os.remove(filepath)
                raise e

            finally:
                elapsed = time() - start_time
                logging.info(
                    f'{picked.name} downloaded {(read_amt / 1000):.2f} KB '
                    f'in {elapsed:.4f} secs')

        except Exception as e:
            logging.error(e)



    async def _run_selection(self) -> File.Data:
        """
        Get all possible files and select one of the files for downloading
        """
        logging.debug('getting download options')

        start_time = time()
        files = await self._system_files()
        elapsed = time() - start_time

        logging.info(
            f"response time for getting files took {elapsed:.4f} secs")

        if not files:
            print('There are no files that can be downloaded')
            raise RuntimeError('no files can be downloaded')

        picked = await self._select_file(files)

        if not picked:
            print('invalid file entered')
            raise RuntimeError('invalid file selected')

        return picked


    async def _select_file(
        self, file_data: Iterable[File.Data]) -> Optional[File.Data]:
        """ Take in user input to select a file from the given set """
        file_map = { f.name: f for f in file_data }
        file_names = sorted(file_map.keys()) # sort might be slow

        options = [f'{i+1}: {file}' for i, file in enumerate(file_names)]
        options.append("enter file to download: ")

        choice = await ainput('\n'.join(options))
        logging.info(f'{len(file_names)} options, {choice=}')

        if choice.isnumeric():
            idx = int(choice)-1
            if idx >= 0 and idx < len(file_names):
                choice = file_names[idx]

        if choice in file_map:
            return file_map[choice]
        else:
            return None


    async def _get_response(self, picked: File.Data) -> Response:
        """ Gets response info for a given file data """
        start_time = time()
        response = await self._requests.wait_for_response(picked)
        elapsed = time() - start_time

        logging.info(f"query {picked} took {elapsed:.4f} secs")

        self_loc = Location(socket.gethostname(), self._port)
        other_loc = frozenset(
            loc for loc in response.locations if loc != self_loc)

        if not other_loc:
            print(f'Cannot find download target for {picked.name}')
            raise RuntimeError(f"location for {picked} is only self")

        return Response(response.file, other_loc)


    def _new_download_path(self, response: Response) -> Path:
        """
        Gets a non-existing path based on the file, and truncates to the
        expected response size
        """
        filepath = self._direct.joinpath(response.file.name)
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

        return filepath



    @staticmethod
    def _chunk_number(chunk: File.Chunk):
        """ The order number of chunk with respect to target file """
        return chunk.offset // Peer.CHUNK_SIZE


    async def _download_to_path(self, filepath: Path, response: Response):
        """
        Connects to the peers in response, and download the information to 
        the filepath
        """
        target_map = self._split_to_chunks(response)
        downloads = [
            self._peer_download(filepath, target, *chunks)
            for target, chunks in target_map.items() ]

        results = await aio.gather(*downloads, return_exceptions=True)

        if any( isinstance(r, Exception) for r in results ):
            print('Ran into an issue while downloading')
            raise RuntimeError('issue while downloading')

        if not self._checksum_passed(filepath, results):
            raise RuntimeError('checksum failed')

        logging.info('checksum passed')


    def _split_to_chunks(
        self,
        response: Response) -> Mapping[Location, Iterable[File.Chunk]]:
        """ Divides a response into location chunk pairings """

        num_chunks = response.file.size // Peer.CHUNK_SIZE
        remaining = response.file.size % Peer.CHUNK_SIZE

        chunks = [
            File.Chunk(
                name = response.file.name,
                offset = i * Peer.CHUNK_SIZE,
                size = Peer.CHUNK_SIZE)
            for i in range(num_chunks) ]

        chunks.append( # add leftover that is not a full chunk
            File.Chunk(
                name = response.file.name,
                offset = num_chunks * Peer.CHUNK_SIZE,
                size = remaining))

        # don't filter for self location, should be done before
        locations = list(response.locations)
        chunk_targets = defaultdict[Location, list[File.Chunk]](list)

        for i, chunk in enumerate(chunks):
            loc = locations[i % len(locations)]
            chunk_targets[loc].append(chunk)

        return chunk_targets


    def _checksum_passed(
        self,
        filepath: Path,
        results: list[Optional[bytes]]) -> bool:
        """ Checks if the remote checksum matches the local checksum """

        checksum = [r for r in results if isinstance(r, bytes)]
        if not checksum:
            return False

        local_checksum = hashlib.md5()

        with open(filepath, 'rb') as f:
            for line in f: local_checksum.update(line)

        local_checksum = local_checksum.digest()
        checksum = checksum[0]

        return checksum == local_checksum


    async def _peer_download(
        self,
        filepath: Path,
        target: Location,
        *chunks: File.Chunk) -> Optional[bytes]:
        """ Client-side connection with any of the other peers """

        log = logging.getLogger()
        try:
            self._check_chunks(target, *chunks)

            logging.debug(f"attempting connection to {target}")
            pair = await aio.open_connection(target.host, target.port)
            pair = StreamPair(*pair)
            reader, writer = pair

            try:
                await Message.write(writer, self._user)
                user = await Message[str].read(reader)

                log = logging.getLogger(user)
                log.debug("connected as client")

                return await self._download_requests(
                    pair, filepath, *chunks, log=log)

            finally:
                if not reader.at_eof():
                    writer.write_eof()

                writer.close()
                await writer.wait_closed()
                log.debug("disconnected as client")
            
        except Exception as e:
            log.exception(e)
            raise e


    def _check_chunks(self, target: Location, *chunks: File.Chunk):
        """ Verify that all chunks have some given assumptions """
        self_loc = Location(socket.gethostname(), self._port)
        if target == self_loc:
            raise ValueError('location specified is self')

        if not chunks:
            raise ValueError('no chunks specified')

        if not all(c.name == chunks[0].name for c in chunks):
            raise ValueError('chunks do not have same file name')


    async def _download_requests(
        self,
        peer: StreamPair,
        filepath: Path,
        *chunks: File.Chunk,
        log: logging.Logger) -> Optional[bytes]:
        """ Send download and checksum requests to the peer connection """

        chunk_nums = [Peer._chunk_number(c) for c in chunks]

        with open(filepath, 'r+b') as f:
            for num, chunk in zip(chunk_nums, chunks):
                # get each chunk one by one per connection
                start_time = time()
                await self._receive_file_retries(f, peer, chunk)
                elapsed = time() - start_time

                log.debug(
                    f'received chunk {num} of {filepath.name}'
                    f'in {elapsed:.4f} secs')

        if any(n == 0 for n in chunk_nums):
            checksum = await peer.request(
                Request.CHECK,
                filename = chunks[0].name,
                as_type = bytes)

            return checksum


    async def _receive_file_retries(
        self,
        file_ptr: BufferedRandom,
        peer: StreamPair,
        chunk: File.Chunk):
        """
        Runs multiple attempts to download a file from the server if on
        download failure
        """
        read_size = False
        num_tries = 0

        async def file_receiver(reader: aio.StreamReader):
            """ Used by the client to receive file content """
            file_data = await Message[bytes].read(reader) # potential stack issue
            file_ptr.seek(chunk.offset)
            amt_got = file_ptr.write(file_data)
            return amt_got

        while not read_size and num_tries < 5:
            # TODO: make tries param
            amt_read = await peer.request(
                Request.DOWNLOAD,
                chunk = chunk,
                receiver = file_receiver )

            read_size = amt_read == chunk.size
            num_tries += 1

    #endregion


    async def _peer_upload(self, peer: StreamPair):
        """ Server-side connection with any of the other peers """
        reader, writer = peer
        log = logging.getLogger()

        try:
            user = await Message[str].read(reader)
            await Message.write(writer, self._user)

            log = logging.getLogger(user)
            log.debug(f"connected as server")

            while procedure := await Message[Procedure].read(reader):
                request = procedure.request

                if request is Request.CHECK:
                    await procedure(self._send_checksum, peer)

                elif request is Request.DOWNLOAD:
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
            log.debug(f"disconnected as server")


    async def _send_checksum(self, peer: StreamPair, filename: str):
        """ Sends the checksum of the given filename """
        writer = peer.writer
        filepath = self._direct.joinpath(filename)

        checksum = hashlib.md5()
        with open(filepath, 'rb') as f:
            for line in f: checksum.update(line)
            
        checksum = checksum.digest()
        await Message[bytes].write(writer, checksum)
            

    async def _send_file(
        self, peer: StreamPair, chunk: File.Chunk, log: logging.Logger):
        """
        Runs an attempts to send a file to a peer
        """
        start_time = time()
        writer = peer.writer
        filepath = self._direct.joinpath(chunk.name)

        with open(filepath, 'rb') as f:
            f.seek(chunk.offset)
            send = f.read(chunk.size) # potential stack issue
            await Message.write(writer, send)
            # await writer.writelines(chunk_iter(f, chunk.size))

        elapsed = time() - start_time
        chunk_num = Peer._chunk_number(chunk)
        log.debug(
            f'sent {chunk.name} chunk #{chunk_num}: '
            f'{(chunk.size/1000):.2f} KB in {elapsed:.5f} secs')



def init_log(log: str, verbosity: int, **_):
    """ Specifies logging format and location """
    log_path = Path(f'./{log}').with_suffix('.log')
    log_path.parent.mkdir(exist_ok=True, parents=True)

    log_settings = dict[str, Union[str, int]](
        format = "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s: %(message)s",
        datefmt = "%H:%M:%S",
        level = logging.getLevelName(verbosity) )

    if not log_path.exists() or log_path.is_file():
        logging.basicConfig(filename=log, filemode='w', **log_settings)
    else: # just use stdout
        logging.basicConfig(**log_settings)


if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser(description='creates a peer node')

    args.add_argument('-a', '--address',
        help = 'ip address of the indexing server')

    args.add_argument('-c', '--config',
        help = 'base arguments on a config file, other args will be ignored')

    args.add_argument('-d', '--directory',
        default = '',
        help = 'the client download folder')

    args.add_argument('-i', '--in-port',
        type = int,
        default = 8889,
        help = 'the port of the indexer')

    args.add_argument('-l', '--log',
        default = 'weak.log',
        help = 'the file to write log info to')

    args.add_argument('-p', '--port',
        type = int,
        default = 9989,
        help = 'the port to listen for connections')

    args.add_argument('-u', '--user',
        help = 'username of the client connecting')

    args.add_argument('-v', '--verbosity',
        type = int,
        choices = [0, 10, 20, 30, 40, 50],
        default = 10,
        help = 'the logging verboseness, level corresponds to default levels')

    args = args.parse_args()
    args = merge_config_args(args)

    init_log(**args)

    peer = Peer(**args)
    aio.run(peer.start_server())
