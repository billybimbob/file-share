#!/usr/bin/env python3

from __future__ import annotations
from typing import Optional, Union
from collections.abc import Iterable
from dataclasses import dataclass, field

from functools import partial
from argparse import ArgumentParser
from pathlib import Path
from time import time

import asyncio as aio
import socket
import hashlib
import re
import logging

from connection import (
    CHUNK_SIZE,
    Message, Procedure, Request, StreamPair, Login,
    ainput, merge_config_args, version_check
)


@dataclass(frozen=True)
class IndexState:
    """
    All variable and constant information about the indexer connection
    """
    info: tuple[str, int]
    pair: StreamPair
    access: aio.Lock = field(default_factory=aio.Lock)



class Peer:
    """ Represents a single peer node """
    _port: int
    _user: str
    _direct: Path
    _index_start: tuple[str, int]
    _dir_update: aio.Event

    PROMPT = (
        "1. List all available files in system\n"
        "2. Download a file\n"
        "3. Kill Server (any value besides 1 & 2 also works)\n"
        "Select an Option: ")


    def __init__(
        self,
        port: int,
        user: Optional[str] = None,
        address: Optional[str] = None, 
        in_port: Optional[int] = None,
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

        if in_port is None:
            in_port = self._port

        self._index_start = (in_host, in_port)


    def get_files(self) -> frozenset[str]:
        """ Gets all the files from the direct path attribute """
        return frozenset(
            p.name
            for p in self._direct.iterdir()
            if p.is_file()
        )


    async def start_server(self):
        """
        Connects the peer to the indexing node, and starts the peer server
        """
        # async sync state needs to be init from async context
        self._dir_update = aio.Event()
        
        def to_upload(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Indexer closure as a stream callback """
            return self._peer_upload(StreamPair(reader, writer))

        try:
            host = socket.gethostname()
            server = await aio.start_server(to_upload, host, self._port, start_serving=False)
            logging.info(f'peer server on {host}, port {self._port}')

            async with server:
                if not server.sockets:
                    return 

                super_peer = await self._connect_super(host)

                # make sure that event is listening before it can be set and cleared
                first_update = aio.create_task(self._dir_update.wait())

                sender = aio.create_task(self._send_dir(super_peer))
                checker = aio.create_task(self._check_dir())

                # only accept connections and user input after a dir_update
                # should not deadlock, keep eye on
                await first_update
                await server.start_serving()

                session = aio.create_task(self._session(super_peer))
                conn = aio.create_task(self._watch_connection(super_peer))

                await aio.wait([session, conn], return_when=aio.FIRST_COMPLETED)

                checker.cancel()
                sender.cancel()

                if not session.done(): session.cancel()
                if not conn.done(): conn.cancel()

            await server.wait_closed()

        except Exception as e:
            logging.exception(e)
            for task in aio.all_tasks():
                task.cancel()

        logging.debug("disconnected from indexing server")
        logging.debug("ending peer")


    async def _connect_super(self, host: str) -> IndexState:
        start = self._index_start

        logging.info(f'trying connection with indexer at {start}')

        conn = aio.create_task(aio.open_connection(*start))
        fin, pend = await aio.wait({conn}, timeout=8)
        for p in pend: p.cancel()

        if len(fin) == 0:
            raise aio.TimeoutError("Connection took to long")

        in_pair = fin.pop().result()
        in_pair = StreamPair(*in_pair)

        login = Login(self._user, host, self._port)
        await Message.write(in_pair.writer, login)

        return IndexState(start, in_pair)


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
                    self._dir_update.set()

                await aio.sleep(5) # TODO: make interval param

        except aio.CancelledError:
            pass


    async def _send_dir(self, indexer: IndexState):
        """
        Sends updated directory to indexer, should be the only place where files attr 
        is updated in order to sync with indexer
        """
        try:
            while True:
                await self._dir_update.wait()
                async with indexer.access:
                    await indexer.pair.request(Request.UPDATE, files=self.get_files())
                    # should be only place where dir_update is cleared
                    self._dir_update.clear()

        except aio.CancelledError:
            pass


    async def _watch_connection(self, indexer: IndexState):
        """ Coroutine that completes when the indexer connection is closed """
        reader, writer = indexer.pair
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
                        print(f'Exiting {self._user} peer server')
                        break

        except aio.CancelledError:
            pass

        except Exception as e:
            logging.exception(e)

        logging.info(f"{self._user} session ending")



    async def _system_files(self, indexer: IndexState) -> list[str]:
        """ Fetches all the files in the system based on indexer """
        files = await indexer.pair.request(Request.FILES, as_type=frozenset[str])
        return sorted(files)

    
    async def _list_system(self, indexer: IndexState):
        """ Fetches then prints the system files """
        files = await self._system_files(indexer)
        print('The files on the system:')
        print('\n'.join(files))



    async def _run_download(self, indexer: IndexState):
        """ Queries the indexer, prompts the user, then fetches file from peer """
        start_time = time()
        files = await self._system_files(indexer)
        get_elapsed = time() - start_time

        if len(files) == 0:
            print('There are no files that can be downloaded')
            logging.exception("no files can be downloaded")
            return

        picked = await self._select_file(files)

        # query for loc
        start_time = time()
        peers = await indexer.pair.request(
            Request.QUERY,
            filename=picked,
            as_type=set[tuple[str, int]]
        )

        query_elapsed = time() - start_time
        elapsed = (get_elapsed + query_elapsed) / 2
        logging.info(f"query for {picked} took {elapsed:.4f} secs")

        self_loc = socket.gethostname(), self._port
        at_self = True
        target = None

        while at_self and len(peers) > 0:
            peer = peers.pop()
            at_self = self_loc == peer
            if not at_self:
                target = peer

        if target is None:
            if at_self:
                logging.error(f'location for {picked} is in own peer')
            else:
                logging.error(f"location for {picked} cannot be found")
            return
        
        # use a random peer target
        await self._peer_download(picked, target)
        self._dir_update.set()


    async def _select_file(self, fileset: Iterable[str]) -> str:
        """ Take in user input to select a file from the given set """
        # sort might be slow
        files = sorted(fileset)
        options = (f'{i+1}: {file}' for i, file in enumerate(files))

        print('\n'.join(options))
        choice = await ainput("enter file to download: ")

        if choice.isnumeric():
            idx = int(choice)-1
            if idx >= 0 and idx < len(files):
                choice = files[idx]

        if choice not in fileset:
            print('invalid file entered')
        
        return choice

    
    async def _peer_download(self, filename: str, target: tuple[str, int]):
        """ Client-side connection with any of the other peers """
        logging.debug(f"attempting connection to {target}")

        fin, pend = await aio.wait({aio.open_connection(*target)}, timeout=8)
        for p in pend: p.cancel()

        if len(fin) == 0:
            logging.error('connection attempt failed')
            return

        pair = fin.pop().result() # will only be one result
        pair = StreamPair(*pair)
        reader, writer = pair

        log = logging.getLogger()

        try:
            await Message.write(writer, self._user)
            user = await Message[str].read(reader)

            log = logging.getLogger(user)
            log.debug("connected")
            
            await self._receive_file_retries(filename, pair, log)

        except Exception as e:
            logging.exception(e)

        finally:
            if not reader.at_eof():
                writer.write_eof()

            writer.close()
            await writer.wait_closed()
            log.debug("disconnected")



    async def _receive_file_retries(self, filename: str, peer: StreamPair, log: logging.Logger):
        """ Runs multiple attempts to download a file from the server if needed """
        start_time = time()
        # reader, _ = peer

        filepath = self._direct.joinpath(filename)
        exts = "".join(filepath.suffixes)
        stem = filepath.stem

        # can have issues with different files having the same name
        if m := re.match(r'\)(\d+)\(', stem[::-1]):
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


    async def _send_file(self, peer: StreamPair, filename: str, log: logging.Logger):
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
    log_path = Path(f'./{log}')
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
    args.add_argument("-i", "--in_port", type=int, default=8888, help="the port of the indexing server")
    args.add_argument("-l", "--log", default='peer.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8889, help="the port to listen for connections")
    args.add_argument("-u", "--user", help="username of the client connecting")
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")

    args = args.parse_args()
    args = merge_config_args(args)

    init_log(**args)

    peer = Peer(**args)
    aio.run(peer.start_server())