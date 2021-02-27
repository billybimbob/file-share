import sys
from socket import gethostname
import asyncio as aio
import hashlib

from pathlib import Path
import argparse
import logging


CHUNK_SIZE = 1024
CLIENT_PROMPT = """
1. List files on server
2. Download file
3. Exit
Select an Option: """

#region Client functions
    
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


async def ainput(prompt: str) -> str:
    """Async version of user input"""
    await aio.get_event_loop().run_in_executor(None, lambda: sys.stdout.write(prompt))
    return await aio.get_event_loop().run_in_executor(None, sys.stdin.readline)



async def client_session(reader: aio.StreamReader, writer: aio.StreamWriter, path: Path):
    """Procedure on how the client interacts with the server"""
    logging.debug('connected client')

    try:
        while option := await aio.wait_for(ainput(CLIENT_PROMPT), 10):
            option = option.rstrip()

            if option == '1':
                await list_dir(option, reader, writer)
            elif option == '2':
                await run_download(option, reader, writer)
            else:
                break

    except aio.TimeoutError:
        print('\ntimeout ocurred')
        pass


async def list_dir(option: str, reader: aio.StreamReader, writer: aio.StreamWriter):
    """Fetches and prints the files from the server"""
    writer.write(f'{option}\n'.encode())
    await writer.drain()
    dirs = await receive_dirs(reader)
    print("The files on the server are:")
    print(dirs)


async def run_download(option: str, reader: aio.StreamReader, writer: aio.StreamWriter):
    """Runs and selects files to download from the server"""
    writer.write(f'{option}\n'.encode())
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

    writer.write(f'{filename}\n'.encode())
    await writer.drain()

    await receive_file(f'{path.name}/{filename}', reader)



async def receive_file(filepath: str, reader: aio.StreamReader):
    """ Used by the client side to download and verify correctness of download"""
    # expect the checksum to be sent first
    checksum = await checked_readline(reader)
    checksum = checksum.decode().strip()

    with open(filepath, 'w+b') as f: # overrides existing
        while True:
            chunk = await checked_read(reader, CHUNK_SIZE)
            f.write(chunk)
            if len(chunk) < CHUNK_SIZE:
                break

        f.seek(0)
        local_checksum = hashlib.md5()
        for line in f:
            local_checksum.update(line)
        
        local_checksum = local_checksum.hexdigest()
        logging.debug(f'checksum of: {local_checksum} vs {checksum}')

        if local_checksum != checksum:
            logging.error("checksum failed")
            raise aio.CancelledError
        else:
            logging.debug("checksum passed")



async def receive_dirs(reader: aio.StreamReader) -> str:
    """Attempt to read multiple lines of file names from the reader"""
    file_names = await checked_read(reader, CHUNK_SIZE)
    file_names = file_names.decode()
    return file_names

#endregion



async def open_connection(port: int, path: Path, host: str=None):
    """Attempts to connect to a server with the given args"""
    if host is None:
        # should be loopback
        host = gethostname()

    has_connect = False
    try:
        reader, writer = await aio.open_connection(host, port)
        has_connect = True
        await client_session(reader, writer, path)

    except Exception as e:
        logging.error(f"connection error {e}")

    finally:
        logging.debug('ending connection')
        if has_connect:
            writer.close()
            await writer.wait_closed()




if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    args = argparse.ArgumentParser("creates a client and connect a server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--dir", default='', help="the client download folder")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port connect to the server")
    # args.add_argument("-s", "--server", action="store_true", help="create a server, will default to a client connection")
    args = args.parse_args()

    # todo: account for config
    path = Path(f'./{args.dir}') # ensure relative path
    path.mkdir(exist_ok=True)

    aio.run(open_connection(args.port, path))
