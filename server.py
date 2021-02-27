import socket
import asyncio as aio
import hashlib

from pathlib import Path
import argparse
import logging

from typing import Callable, Awaitable, Dict


SERVER_PROMPT = """
1. List files on server
2. Delete a file
3. Kill Server
Select an Option: 
"""

#region Server functions

async def server_session(path: Path):
    while option := input(SERVER_PROMPT):
        if option == '1':
            pass
        elif option == '2':
            pass
        else:
            break


async def server_connection(reader: aio.StreamReader, writer: aio.StreamWriter, path: Path):
    """Callback called for the server wheneven a connection is established with a client"""
    addr = writer.get_extra_info('peername')
    remote = socket.gethostbyaddr(addr[0])

    log = logging.getLogger(remote[0])
    default_logger(log)

    log.debug(f"connected to {remote}")

    try:
        while request := await reader.readuntil():
            request = request.decode().strip()

            if request == '1':
                log.debug("listing dir")
                await list_dir(path, writer)

            elif request == '2':
                await list_dir(path, writer)
                log.debug("waiting for selected file")
                filename = await reader.readuntil()
                filename = filename.decode().rstrip()
                await send_file(f'{path.name}/{filename}', writer, log)

            else:
                break

    except EOFError:
        pass

    except IOError as e:
        log.error(e)
        writer.write(str(e).encode())

    finally:
        log.debug('ending connection')
        writer.write_eof()
        await writer.drain()
        await writer.wait_closed()



def path_connection(path: Path) -> Callable[[aio.StreamReader, aio.StreamReader], Awaitable]:
    """Creates a server connection, and specifying the path for the server directory"""
    return lambda reader, writer: server_connection(reader, writer, path)



async def send_file(filepath: str, writer: aio.StreamWriter, log: logging.Logger):
    """Used by server side to send file contents to a given client"""
    log.debug(f'trying to send file {filepath}')

    with open(filepath, 'rb') as f:
        checksum = hashlib.md5()
        for line in f:
            checksum.update(line)
        checksum = checksum.hexdigest()

        log.info(f'checksum of: {checksum}')

        writer.write(f'{checksum}\n'.encode())
        await writer.drain()

        f.seek(0)
        writer.writelines(f) # don't need to encode
        await writer.drain()



async def list_dir(directory: Path, writer: aio.StreamWriter):
    """Sends the server directory files to the client"""
    file_list = '\n'.join(d.name for d in directory.iterdir() if d.is_file())
    writer.write(file_list.encode())
    await writer.drain()

#endregion



async def start_stream(port: int, path: Path):
    """Switch to convert the command args to a given server or client"""
    host = socket.gethostname() # should be loopback

    server = await aio.start_server(path_connection(path), host=host, port=port)
    async with server:
        addr = server.sockets[0].getsockname()
        logging.debug(f'created server on {addr}, listening for clients')

        # aio.create_task(server_session(path))
        await server.serve_forever()
 

def default_logger(log: logging.Logger):
    log.setLevel(logging.DEBUG)


if __name__ == "__main__":
    default_logger(logging.getLogger())

    args = argparse.ArgumentParser("creates a server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--dir", default='', help="the directory to where the server hosts files")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")
    # args.add_argument("-s", "--server", action="store_true", help="create a server, will default to a client connection")
    args = args.parse_args()

    # todo: account for config
    path = Path(f'./{args.dir}') # ensure relative path
    path.mkdir(exist_ok=True)

    try:
        aio.run(start_stream(args.port, path))
    except KeyboardInterrupt:
        logging.debug("server is stopped")
