from argparse import ArgumentParser
from pathlib import Path
from time import time

import socket
import asyncio as aio
import hashlib
import logging

import serverbase as defs



CLIENT_PROMPT = """\
1. List files on server
2. Download file
3. Exit
Select an Option: """
    

async def checked_readline(reader: aio.StreamReader):
    """
    Calls readline on the reader, and assumes that if at eof,
    an exception was passed
    """
    res = await reader.readline()
    if reader.at_eof():
        raise RuntimeError(res.decode())
    else:
        return res


async def checked_read(reader: aio.StreamReader, n: int):
    """
    Calls read on the reader, and assumes that if at eof,
    an exception was passed
    """
    res = await reader.read(n)
    if reader.at_eof():
        raise RuntimeError(res.decode())
    else:
        return res


async def client_session(path: Path, reader: aio.StreamReader, writer: aio.StreamWriter):
    """Procedure on how the client interacts with the server"""
    logging.debug('connected client')

    try:
        while option := await aio.wait_for(defs.ainput(CLIENT_PROMPT), 30):
            option = option.rstrip()
            try:
                if option == '1':
                    await list_dir(reader, writer)
                elif option == '2':
                    await run_download(path, reader, writer)
                else:
                    if option and option!='3':
                        logging.error("unknown command")
                    print('closing client')
                    break

            except RuntimeError as e:
                logging.error(f"error from server: {e}")

    except aio.TimeoutError:
        print('\ntimeout ocurred')
    
    finally:
        writer.close()
        await writer.wait_closed()



async def list_dir(reader: aio.StreamReader, writer: aio.StreamWriter):
    """Fetches and prints the files from the server"""
    writer.write(f'{defs.GET_FILES}\n'.encode())
    await writer.drain()
    dirs = await receive_dirs(reader)
    print("The files on the server are:")
    print(f"{dirs}\n")



async def run_download(path: Path, reader: aio.StreamReader, writer: aio.StreamWriter):
    """Runs and selects files to download from the server"""
    writer.write(f'{defs.GET_FILES}\n'.encode())
    await writer.drain()

    dirs = await receive_dirs(reader)
    dirs = dirs.splitlines()
    dir_options = (f'{i+1}: {file}' for i, file in enumerate(dirs))

    print('\n'.join(dir_options))
    selected = input("enter file to download: ")

    filename = selected
    if selected.isnumeric():
        idx = int(selected) - 1 
        if idx >= 0 and idx < len(dirs):
            filename = dirs[idx]

    await receive_file_new(filename, path, writer)



async def receive_file_same(filename: str, path: Path, reader: aio.StreamReader, writer: aio.StreamWriter):
    """Basic form of timing and downloading from the server"""
    start_time = time()
    await receive_file_loop(filename, path, reader, writer)
    await send_transfer_time(start_time, filename, writer)



async def receive_file_new(filename: str, path: Path, writer_parent: aio.StreamWriter):
    """Creates a new connection with the server, and downloads the filename"""
    start_time = time()
    have_connection = False
    try:
        addr, port, _, _ = writer_parent.get_extra_info('peername')
        host, _, _ = socket.gethostbyaddr(addr)

        # each file transfer is its own connection
        reader, writer = await aio.open_connection(host, port)
        have_connection = True

        await receive_file_loop(filename, path, reader, writer)
        await send_transfer_time(start_time, filename, writer)

    except Exception as e:
        raise e
    finally:
        if have_connection:
            writer.close()
            await writer.wait_closed()



async def send_transfer_time(start_time: float, filename: str, writer: aio.StreamWriter):
    """Determines elapsed time, and shares with the server"""
    elapsed = time() - start_time
    logging.debug(f'transfer time of {filename} was {elapsed:.4f} secs')

    writer.write(f'{elapsed}\n'.encode())
    await writer.drain()


async def receive_file_loop(filename: str, path: Path, reader: aio.StreamReader, writer: aio.StreamWriter):
    """Runs multiple attempts to download a file from the server"""
    writer.write(f'{defs.DOWNLOAD}\n'.encode())
    await writer.drain()

    writer.write(f'{filename}\n'.encode())
    await writer.drain()

    got_file = False
    num_tries = 0
    while not got_file and num_tries < 5:
        if num_tries > 0:
            writer.write(f'{defs.RETRY}\n'.encode())
            await writer.drain()

        got_file = await receive_file(f'{path.name}/{filename}', reader)
        num_tries += 1
    
    writer.write(f'{defs.SUCCESS}\n'.encode())
    await writer.drain()



async def receive_file(filepath: str, reader: aio.StreamReader) -> bool:
    """ Used by the client side to download and verify correctness of download"""
    # expect the checksum to be sent first
    checksum = await checked_readline(reader)
    checksum = checksum.decode().strip()
    checksum_passed = False

    with open(filepath, 'w+b') as f: # overrides existing
        while True:
            chunk = await checked_read(reader, defs.CHUNK_SIZE)
            f.write(chunk)
            if len(chunk) < defs.CHUNK_SIZE:
                break

        f.seek(0)
        local_checksum = hashlib.md5()
        for line in f:
            local_checksum.update(line)
        local_checksum = local_checksum.hexdigest()

        checksum_passed = local_checksum == checksum
        if checksum_passed:
            logging.debug("checksum passed")
        else:
            logging.error("checksum failed")

    return checksum_passed




async def receive_dirs(reader: aio.StreamReader) -> str:
    """Attempt to read multiple lines of file names from the reader"""
    file_names = await checked_read(reader, defs.CHUNK_SIZE)
    file_names = file_names.decode()
    return file_names



async def open_connection(host: str, port: int, path: Path):
    """Attempts to connect to a server with the given args"""
    if host is None:
        # should be loopback
        host = socket.gethostname()

    try:
        reader, writer = await aio.open_connection(host, port)
        await client_session(path, reader, writer)
    except Exception as e:
        logging.error(f"connection error {e}")
    finally:
        logging.debug('ending connection')



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    args = ArgumentParser("creates a client and connect a server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--dir", default='', help="the client download folder")
    args.add_argument("-i", "--address", default=None, help="ip address of the server")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port connect to the server")
    args = args.parse_args()

    # todo: account for config
    path = Path(f'./{args.dir}') # ensure relative path
    path.mkdir(exist_ok=True)
    host = socket.gethostbyaddr(args.address) if args.address else None 

    aio.run(open_connection(host, args.port, path))
