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
    res = await reader.readline()
    if reader.at_eof():
        raise RuntimeError(res.decode())
    else:
        return res


async def checked_read(reader: aio.StreamReader, n: int):
    res = await reader.read(n)
    if reader.at_eof():
        raise RuntimeError(res.decode())
    else:
        return res


async def client_session(reader: aio.StreamReader, writer: aio.StreamWriter, path: Path):
    """Procedure on how the client interacts with the server"""
    logging.debug('connected client')

    while option := input(CLIENT_PROMPT):
        if option == '1':
            writer.write(f'{option}\n'.encode())
            await writer.drain()
            dirs = await receive_dirs(reader)
            print("The files on the server are:")
            print(dirs)

        elif option == '2':
            writer.write(f'{option}\n'.encode())
            await writer.drain()

            dirs = await receive_dirs(reader)
            dirs = dirs.splitlines()
            dir_options = (f'{i+1}: {file}' for i, file in enumerate(dirs))

            print('\n'.join(dir_options))
            selected = input("enter file to download: ")

            if selected.isnumeric():
                idx = int(selected) - 1 
                if idx >= 0 and idx < len(dirs):
                    filename = dirs[idx]
                else:
                    filename = selected
            else:
                filename = selected

            writer.write(f'{filename}\n'.encode())
            await writer.drain()

            await receive_file(f'{path.name}/{filename}', reader)

        else:
            break



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
    file_names = await checked_read(reader, CHUNK_SIZE)
    file_names = file_names.decode()
    return file_names

#endregion



async def start_stream(port: int, path: Path):
    """Switch to convert the command args to a given server or client"""
    host = gethostname() # should be loopback
    try:
        reader, writer = await aio.open_connection(host, port)
        await client_session(reader, writer, path)

    except Exception as e:
        logging.error(f"connection error {e}")

    finally:
        logging.debug('ending connection')
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

    aio.run(start_stream(args.port, path))
