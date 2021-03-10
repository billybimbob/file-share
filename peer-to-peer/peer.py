#!/usr/bin/env python3

from __future__ import annotations
from typing import Any, NamedTuple, Optional, Union
from dataclasses import dataclass, field

from argparse import ArgumentParser
from pathlib import Path

import asyncio as aio
import socket
import hashlib
import logging

from connection import (
    Message, Request, StreamPair,
    ainput, merge_config_args, version_check
)


class IndexInfo(NamedTuple):
    """ Information to connect to the index node """
    host: str
    port: int


@dataclass
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
    files: set[str]
    index_start: IndexInfo

    PROMPT = "" \
        "1. List all available files in system" \
        "2. Download a file" \
        "Select an Option: "


    def __init__(
        self,
        port: int,
        user: Optional[str]=None,
        address: Optional[str]=None, 
        in_port: Optional[int]=None,
        directory: Union[str, Path, None]=None,
        *args: Any,
        **kwargs: Any):

        self.port = min(1, port)
        self.user = socket.gethostname() if user is None else user

        if directory is None:
            self.direct = Path('.')
        elif isinstance(directory, str):
            self.direct = Path(f'./{directory}')
        else:
            self.direct = directory

        self.direct.mkdir(exist_ok=True, parents=True)

        if address is None:
            in_host = socket.gethostname()
        else:
            in_host = socket.gethostbyaddr(address)[0]

        if in_port is None:
            in_port = self.port

        self.index_start = IndexInfo(in_host, in_port)


    async def start_server(self):
        """
        Connects the peer to the indexing node, and starts the peer server
        """
        def to_upload(reader: aio.StreamReader, writer: aio.StreamWriter):
            """ Indexeder closure as a stream callback """
            return self.upload(StreamPair(reader, writer))

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

                dir_updates = aio.create_task(self.check_dir_updates(indexer))
                aio.create_task(server.serve_forever())

                await aio.create_task(self.session(indexer))
                dir_updates.cancel()

        await server.wait_closed()


    async def check_dir_updates(self, indexer: IndexState):
        """
        Polls on directory file name changes, and sends updates to indexer
        """
        try:
            _, writer = indexer.pair
            while True:
                check = { file.name for file in self.direct.iterdir() }
                if self.files != check:
                    self.files = check
                    filenames = list(self.files)

                    async with indexer.access:
                        await Message.write(writer, Request.UPDATE)
                        await Message.write(writer, filenames)

                await aio.sleep(5)

        except aio.CancelledError:
            pass


    async def session(self, indexer: IndexState):
        """ Cli for peer """
        while request := await ainput(Peer.PROMPT):
            async with indexer.access:
                # TODO: make session options
                if request == '1':
                    pass

                elif request == '2':
                    pass

                else:
                    print(f'Exiting {self.user} peer server')
                    break
        

    async def upload(self, peer: StreamPair):
        """ Connection with any of the other peers """
        reader, writer = peer

        info = writer.get_extra_info('peername')
        remote = socket.gethostbyaddr(info[0])
        username = await Message.read(reader, str)

        logging.debug(f"connected to {username}: {remote}")

        try:
            # just do one transaction, and close stream
            request = await Message.read(reader, Request)
            if request != Request.DOWNLOAD:
                raise RuntimeError("Only download requests are possible")

            await self.send_file_loop(peer)

        except Exception as e:
            logging.error(e)
            await Message.write(writer, e)

        finally:
            writer.close()


    async def send_file_loop(self, peer: StreamPair):
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
            await self.send_file(filepath, writer)

            amt_read = await Message.read(reader, int)
            tot_bytes += amt_read

            success = await Message.read(reader, Request)
            should_send = success != Request.SUCCESS

        elapsed = await Message.read(reader, float)
        logging.debug(
            f'transfer of {filename}: {(tot_bytes/1000):.2f} KB in {elapsed:.5f} secs'
        )


    async def send_file(self, filepath: Path, writer: aio.StreamWriter):
        """ Used by server side to send file contents to a given client """
        logging.debug(f'trying to send file {filepath}')

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
    args.add_argument("-l", "--log", default='client.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port connect to the server")
    args.add_argument("-u", "--user", help="username of the client connecting")
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")

    args = args.parse_args()
    args = merge_config_args(args)

    init_log(**args)