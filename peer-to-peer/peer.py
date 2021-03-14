#!/usr/bin/env python3

from __future__ import annotations
from typing import Any, NamedTuple, Optional, Union
from dataclasses import dataclass, field

from argparse import ArgumentParser
from pathlib import Path
from time import time

import asyncio as aio
import socket
import hashlib
import logging

from connection import (
    CHUNK_SIZE, Message, Request, StreamPair,
    ainput, getpeerbystream, merge_config_args, version_check
)


class IndexInfo(NamedTuple):
    """ Information to connect to the index node """
    host: str
    port: int


@dataclass(frozen=True)
class IndexState:
    """
    All variable and constant information about the indexer connection
    """
    info: IndexInfo
    pair: StreamPair
    access: aio.Lock = field(default_factory=aio.Lock)



class Peer:
    """ Represents a single peer node """
    port: int
    user: str
    direct: Path
    dir_update: aio.Event
    index_start: IndexInfo

    PROMPT = (
        "1. List all available files in system\n"
        "2. Download a file\n"
        "3. Kill Server (any value besides 1 & 2 also work)\n"
        "Select an Option: ")


    def __init__(
        self,
        port: int,
        user: Optional[str] = None,
        address: Optional[str] = None, 
        in_port: Optional[int] = None,
        directory: Union[str, Path, None] = None,
        *args: Any,
        **kwargs: Any):
        """
        Creates all the state information for a peer node, use to method
        start_server to activate the peer node
        """
        self.port = max(1, port)
        self.user = socket.gethostname() if user is None else user

        if directory is None:
            self.direct = Path('.')
        elif isinstance(directory, str):
            self.direct = Path(f'./{directory}')
        else:
            self.direct = directory

        self.direct.mkdir(exist_ok=True, parents=True)
        self.dir_update = aio.Event()

        if address is None:
            in_host = socket.gethostname()
        else:
            in_host = socket.gethostbyaddr(address)[0]

        if in_port is None:
            in_port = self.port

        self.index_start = IndexInfo(in_host, in_port)


    def get_files(self) -> frozenset[str]:
        """ Gets all the files from the direct path attribute """
        return frozenset(
            file.name for file in self.direct.iterdir()
        )


    async def start_server(self):
        """
        Connects the peer to the indexing node, and starts the peer server
        """
        def to_upload(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Indexeder closure as a stream callback """
            return self._peer_upload(StreamPair(reader, writer))

        try:
            host = socket.gethostname()
            server = await aio.start_server(to_upload, host, self.port)

            async with server:
                if server.sockets:
                    addr = server.sockets[0].getsockname()[0]
                    logging.info(f'indexing server on {addr}')

                    start = self.index_start
                    in_pair = await aio.open_connection(start.host, start.port)
                    in_pair = StreamPair(*in_pair)

                    indexer = IndexState(start, in_pair)

                    await Message.write(in_pair.writer, self.user)

                    # make sure that event is listening before it can be set and cleared
                    first_update = aio.create_task(self.dir_update.wait())

                    sender = aio.create_task(self._send_dir(indexer))
                    checker = aio.create_task(self._check_dir())

                    # only accept connections and user input after a dir_update
                    # should not deadlock, keep eye on
                    await first_update

                    aio.create_task(server.serve_forever())

                    session = aio.create_task(self._session(indexer))
                    conn = aio.create_task(self._watch_connection(indexer))

                    _, pend = await aio.wait([session, conn], return_when=aio.FIRST_COMPLETED)

                    checker.cancel()
                    sender.cancel()
                    for p in pend: p.cancel()

            await server.wait_closed()

        except Exception as e:
            logging.error(e)



    async def _check_dir(self):
        """
        Polls on directory file name changes, and raises event updates on change found
        """
        try:
            last_check = None
            while True:
                check = self.get_files()
                if last_check != check:
                    last_check = check
                    self.dir_update.set()

                await aio.sleep(5) # TODO: make interval param

        except aio.CancelledError:
            pass


    async def _send_dir(self, indexer: IndexState):
        """
        Sends updated directory to indexer, should be the only place where files attr 
        is updated in order to sync with indexer
        """
        _, writer = indexer.pair
        try:
            while True:
                await self.dir_update.wait()
                async with indexer.access:
                    await Message.write(writer, Request.UPDATE)
                    await Message.write(writer, self.get_files())
                    # should be only place where dir_update is cleared
                    self.dir_update.clear()

        except aio.CancelledError:
            pass


    async def _watch_connection(self, indexer: IndexState):
        """ Coroutine that completes when the indexer connection is closed """
        try:
            check = aio.Condition()
            await check.wait_for(indexer.pair.reader.at_eof)

        except aio.CancelledError:
            # indexer won't be reading, so don't have to eof
            indexer.pair.writer.close()
            await indexer.pair.writer.wait_closed()


    #region session methods

    async def _session(self, indexer: IndexState):
        """ Cli for peer """
        try:
            while request := await ainput(Peer.PROMPT):
                async with indexer.access:
                    if request == '1':
                        await self._list_system(indexer)

                    elif request == '2':
                        await self._run_download(indexer)

                    else:
                        print(f'Exiting {self.user} peer server')
                        break

        except aio.CancelledError:
            pass


    async def _system_files(self, indexer: IndexState) -> list[str]:
        """ Fetches all the files in the system based on indexer """
        await Message.write(indexer.pair.writer, Request.GET_FILES)
        files: list[str] = await Message.read(indexer.pair.reader, list)

        return files

    
    async def _list_system(self, indexer: IndexState):
        """ Fetches then prints the system files """
        files = await self._system_files(indexer)
        print('The files on the system:')
        print('\n'.join(files))



    async def _run_download(self, indexer: IndexState):
        """ Queries the indexer, prompts the user, then fetches file from peer """
        reader, writer = indexer.pair
        files = await self._system_files(indexer)
        files = set(files) - self.get_files()

        if len(files) == 0:
            logging.error("no files can be downloaded")
            return

        picked = self._select_file(files)

        # query for loc
        await Message.write(writer, Request.QUERY)
        await Message.write(writer, picked)

        peers: set[tuple[str, int]] = await Message.read(reader, set)
        if len(peers) == 0:
            logging.error(f"location for {picked} could not be found")
            return
        
        # use a random peer target
        target = peers.pop()
        await self._peer_download(picked, target)

        self.dir_update.set()


    def _select_file(self, fileset: set[str]) -> str:
        """ Take in user input to select a file from the given set """
        # sort might be slow
        files = sorted(fileset)
        options = (f'{i+1}: {file}' for i, file in enumerate(files))

        print('\n'.join(options))
        choice = input("enter file to download: ")

        if choice.isnumeric():
            idx = int(choice)-1
            if idx >= 0 and idx < len(files):
                choice = files[idx]

        if choice not in fileset:
            print('invalid file entered')
        
        return choice

    
    async def _peer_download(self, filename: str, target: tuple[str, int]):
        """ Client-side connection with any of the other peers """
        pair = await aio.open_connection(target[0], target[1])
        pair = StreamPair(*pair)
        writer = pair.writer

        try:
            info = getpeerbystream(writer)
            if not info: return

            remote = socket.gethostbyaddr(info[0])
            logging.debug(f"connected to {remote}")
            
            await Message.write(writer, Request.DOWNLOAD)
            await self._receive_file_loop(filename, pair)

        except Exception as e:
            logging.error(e)

        finally:
            writer.write_eof()
            writer.close()
            await writer.wait_closed()


    async def _receive_file_loop(self, filename: str, peer: StreamPair):
        """ Runs multiple attempts to download a file from the server if needed """
        start_time = time()
        reader, writer = peer

        await Message.write(writer, filename)
        filepath = self.direct.joinpath(filename)

        stem = filepath.stem
        exts = "".join(filepath.suffixes)
        dup_mod = 1
        while filepath.exists():
            filepath = filepath.with_name(f'{stem}({dup_mod}){exts}')
            dup_mod += 1

        got_file = False
        num_tries = 0

        while not got_file and num_tries < 5: # TODO: make param
            if num_tries > 0:
                logging.info(f"retrying {filename} download")
                await Message.write(writer, Request.RETRY)

            got_file, byte_amt = await self._receive_file(filepath, reader)
            await Message.write(writer, byte_amt)
            num_tries += 1
        
        await Message.write(writer, Request.SUCCESS)
        elapsed = time() - start_time

        await Message.write(writer, elapsed)
        logging.debug(f'transfer time of {filename} was {elapsed:.4f} secs')


    async def _receive_file(self, filepath: Path, reader: aio.StreamReader) -> tuple[bool, int]:
        """ Used by the client side to download and verify correctness of download """
        checksum_passed = False
        amt_read = 0

        # expect the checksum to be sent first
        checksum = await Message.read(reader, bytes)

        with open(filepath, 'w+b') as f:
            filesize = await Message.read(reader, int)
            # filesize = int(filesize) # should always be str

            while amt_read < filesize:
                # no messages since each file chunk is part of same "message"
                chunk = await reader.read(CHUNK_SIZE)
                f.write(chunk)
                amt_read += len(chunk)

            logging.info(f'read {(amt_read / 1000):.2f} KB')
            f.seek(0)

            local_checksum = hashlib.md5()
            for line in f:
                local_checksum.update(line)

            local_checksum = local_checksum.digest()
            checksum_passed = local_checksum == checksum

            if checksum_passed:
                logging.info("checksum passed")
            else:
                logging.error("checksum failed")

        return (checksum_passed, amt_read)

    #endregion


    async def _peer_upload(self, peer: StreamPair):
        """ Server-side connection with any of the other peers """
        reader, writer = peer
        try:
            info = getpeerbystream(writer)
            if not info: return

            remote = socket.gethostbyaddr(info[0])
            logging.debug(f"connected to {remote}")

            # just do one transaction, and close stream
            request = await Message.read(reader, Request)
            if request != Request.DOWNLOAD:
                raise RuntimeError("Only download requests are possible")

            await self._send_file_loop(peer)

        except Exception as e:
            logging.error(e)
            await Message.write(writer, e)

        finally:
            writer.write_eof()
            writer.close()
            await writer.wait_closed()


    async def _send_file_loop(self, peer: StreamPair):
        """
        Runs multiple attempts to send a file based on the receiver response
        """
        logging.info("waiting for selected file")
        reader, writer = peer

        filename = await Message.read(reader, str)
        filepath = self.direct.joinpath(filename)

        tot_bytes = 0
        should_send = True

        while should_send:
            await self._send_file(filepath, writer)

            amt_read = await Message.read(reader, int)
            tot_bytes += amt_read

            success = await Message.read(reader, Request)
            should_send = success != Request.SUCCESS

        elapsed = await Message.read(reader, float)
        logging.debug(
            f'transfer of {filename}: {(tot_bytes/1000):.2f} KB in {elapsed:.5f} secs'
        )


    async def _send_file(self, filepath: Path, writer: aio.StreamWriter):
        """ Used by server side to send file contents to a given client """
        logging.debug(f'trying to send file {filepath}')
        # TODO: make logging specific to connection

        with open(filepath, 'rb') as f:
            checksum = hashlib.md5()
            for line in f:
                checksum.update(line)
            checksum = checksum.digest()

            # logger.info(f'checksum of: {checksum}')
            await Message.write(writer, checksum)

            filesize = filepath.stat().st_size
            await Message.write(writer, filesize)

            f.seek(0)
            writer.writelines(f) # don't need to encode
            await writer.drain()
        



def init_log(log: str, verbosity: int, **kwargs: Any):
    """ Specifies logging format and location """
    log_path = Path(f'./{log}')
    log_path.parent.mkdir(exist_ok=True, parents=True)

    log_settings = {
        'format': "%(levelname)s: %(message)s",
        'level': logging.getLevelName(verbosity)
    }

    if not log_path.exists() or log_path.is_file():
        logging.basicConfig(filename=log, **log_settings)
    else: # just use stdout
        logging.basicConfig(**log_settings)


if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser("creates a peer node")
    args.add_argument("-a", "--address", default=None, help="ip address of the indexing server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--directory", default='', help="the client download folder")
    args.add_argument("-i", "--in_port", help="the port of the indxing server")
    args.add_argument("-l", "--log", default='client.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to listen for connections")
    args.add_argument("-u", "--user", help="username of the client connecting")
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")

    args = args.parse_args()
    args = merge_config_args(args)

    init_log(**args)

    peer = Peer(**args)
    aio.run(peer.start_server())