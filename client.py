from typing import Tuple, List, cast
from argparse import ArgumentParser

from pathlib import Path
from time import time

import socket
import asyncio as aio
import hashlib
import logging

from server_defs import (
    Message, StreamPair,
    GET_FILES, DOWNLOAD, SUCCESS, RETRY, CHUNK_SIZE,
    ainput, merge_config_args, version_check
)


CLIENT_PROMPT = """\
1. List files on server
2. Download file
3. Exit (any value besides 1 or 2 also works)
Select an Option: """


async def client_session(
    path: Path, sockets: List[StreamPair], retries: int, timeout: int):
    """ Procedure on how the client interacts with the server """

    logging.info('connected client')
    pair = sockets[0]
    pool: aio.Queue[StreamPair] = aio.Queue(len(sockets)-1)
    for s in sockets[1:]:
        pool.put_nowait(s)

    try:
        while option := await aio.wait_for(ainput(CLIENT_PROMPT), timeout):
            option = option.rstrip()
            try:
                if option == '1':
                    await list_dir(pair)
                elif option == '2':
                    await run_download(path, pair, pool, retries)
                else:
                    # if option and option!='3':
                    #     logging.error("unknown command")
                    print('closing client')
                    break
            except ValueError as e:
                logging.error(e)

    except RuntimeError as e:
        logging.error(f"error from server: {e}")

    except IOError as e:
        logging.error(e)

    except aio.TimeoutError:
        print('\ntimeout ocurred')
    
    finally:
        for socket in sockets:
            socket.writer.close()

        await aio.gather(*[
            s.writer.wait_closed() for s in sockets
        ])



async def list_dir(pair: StreamPair):
    """ Fetches and prints the files from the server """
    await Message.write(pair.writer, GET_FILES)

    dirs = await receive_dirs(pair.reader)
    print("The files on the server are:")
    print(f"{dirs}\n")



async def run_download(path: Path, pair: StreamPair, pool: aio.Queue, retries: int):
    """ Runs and selects files to download from the server """
    await Message.write(pair.writer, GET_FILES)

    dirs = await receive_dirs(pair.reader)
    dirs = dirs.splitlines()
    selects = []

    while len(dirs) > 0: # file selection
        if len(dirs) == 1:
            print("adding last file")
            selects.append(dirs[0])
            break

        dir_options = (f'{i+1}: {file}' for i, file in enumerate(dirs))
        print('\n'.join(dir_options))
        choice = input("enter file to download: ")

        if choice.isnumeric():
            idx = int(choice) - 1 
            if idx >= 0 and idx < len(dirs):
                choice = dirs[idx]
        
        try:
            dirs.remove(choice)
        except:
            raise ValueError('invalid file given')

        selects.append(choice)

        choice = input("download more files? ")
        if choice.startswith('n'):
            break

    if len(selects) <= 1:
        run_parallel = False
    else:
        run_parallel = input("do you want to run the download in parallel? ")
        run_parallel = not run_parallel.startswith('n')

    start_time = time()
    try:
        if run_parallel:
            # concurrent exec
            await aio.gather(*[
                fetch_file_pooled(f, path, pool, retries) for f in selects
            ])
        else:
            for f in selects:
                await fetch_file(f, path, pair, retries)

    except RuntimeError as e:
        logging.error(e)

    elapsed = time() - start_time
    logging.debug(f'total transfer time was {elapsed:.4f} secs')



async def fetch_file(filename: str, path: Path, pair: StreamPair, retries: int):
    """ Basic form of timing and downloading from the server """
    start_time = time()
    await receive_file_loop(filename, path, pair, retries)

    elapsed = time() - start_time
    logging.debug(f'transfer time of {filename} was {elapsed:.4f} secs')
    await Message.write(pair.writer, str(elapsed))



async def fetch_file_pooled(filename: str, path: Path, sockets: aio.Queue, retries: int):
    """ Download a file from the server with on of the pooled sockets """
    start_time = time()
    pair: StreamPair = await sockets.get()
    await receive_file_loop(filename, path, pair, retries)

    elapsed = time() - start_time
    logging.debug(f'transfer time of {filename} was {elapsed:.4f} secs')
    await Message.write(pair.writer, str(elapsed))

    sockets.put_nowait(pair) # should never be > maxsize



async def receive_file_loop(filename: str, path: Path, pair: StreamPair, retries: int):
    """ Runs multiple attempts to download a file from the server if needed """
    reader, writer = pair
    await Message.write(writer, DOWNLOAD)

    await Message.write(writer, filename)
    filepath = path.joinpath(filename)

    stem = filepath.stem
    exts = "".join(filepath.suffixes)
    dup_mod = 1
    while filepath.exists():
        filepath = filepath.with_name(f'{stem}({dup_mod}){exts}')
        dup_mod += 1

    got_file = False
    num_tries = 0
    while not got_file and num_tries < retries:
        if num_tries > 0:
            logging.info(f"retrying {filename} download")
            await Message.write(writer, RETRY)

        got_file, byte_amt = await receive_file(filepath, reader)
        await Message.write(writer, str(byte_amt))
        num_tries += 1
    
    await Message.write(writer, SUCCESS)



async def receive_file(filepath: Path, reader: aio.StreamReader) -> Tuple[bool, int]:
    """ Used by the client side to download and verify correctness of download """
    checksum_passed = False
    amt_read = 0

    # expect the checksum to be sent first
    checksum = await Message.read_raw(reader)

    with open(filepath, 'w+b') as f:
        filesize = await Message.read(reader)
        filesize = int(filesize) # should always be str

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



async def receive_dirs(reader: aio.StreamReader) -> str:
    """ Attempt to read multiple lines of file names from the reader """
    return await Message.read(reader)



def init_log(log: str, verbosity: int):
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




def process_args(user: str, address: str, num_sockets: int, directory: str) \
    -> Tuple[str, str, int, Path]:
    """ Normalize and parse arguments """
    if address is None:
        # should be loopback
        host = socket.gethostname()
    else:
        host, _, _ = socket.gethostbyaddr(address)

    if user is None:
        user = host

    if num_sockets <= 1:
        num_sockets = 2

    path = Path(f'./{directory}') # ensure relative path
    path.mkdir(exist_ok=True, parents=True)

    return (user, host, num_sockets, path)



async def open_connection(
    user: str, address: str, port: int, directory: str, workers: int,
    retries: int, timeout: int, log: str, verbosity: int, *args, **kwargs):
    """ Attempts to connect to a server with the given args """
    try:
        user, host, num_sockets, path = process_args(user, address, workers, directory)
        init_log(log, verbosity)
        sockets: List[StreamPair] = []

        for _ in range(num_sockets):
            pair = await aio.open_connection(host, port)
            pair = StreamPair(*pair)
            sockets.append(pair)
            await Message.write(pair.writer, user)

        await client_session(path, sockets, retries, timeout)

    except Exception as e:
        logging.error(e)

    finally:
        logging.info('ending connection')



if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser("creates a client and connect to a server")
    args.add_argument("-a", "--address", default=None, help="ip address of the server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--directory", default='', help="the client download folder")
    args.add_argument("-l", "--log", default='client.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port connect to the server")
    args.add_argument("-r", "--retries", type=int, default=3, help="amount of download retries on failure")
    args.add_argument("-t", "--timeout", type=int, default=60, help="time in seconds of no activity til the client disconnects")
    args.add_argument("-u", "--user", help="username of the client connecting")
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")
    args.add_argument("-w", "--workers", type=int, default=2, help="max number of sockets that can be connected be connected to the server")

    args = args.parse_args()
    args = merge_config_args(args)

    aio.run( open_connection(**args) )
