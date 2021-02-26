from socket import gethostname
import asyncio as aio
import hashlib

from pathlib import Path
import argparse
import logging

from typing import Callable, Awaitable



#region Server functions

async def server_connection(reader: aio.StreamReader, writer: aio.StreamWriter, path: Path):
    """Callback called for the server wheneven a connection is established with a client"""
    remote = writer.get_extra_info('peername')
    logging.debug(f"connected to {remote}")

    try:
        while option := await reader.readuntil():
            option = option.decode().strip()
            logging.debug(f'got {option=}')

            if option == '1':
                logging.debug("listing dir")
                await list_dir(path, writer)
            elif option == '2':
                logging.debug("fetching file")
                await list_dir(path, writer)

                logging.debug("waiting for selected file")
                filename = await reader.readuntil()
                filename = filename.decode().rstrip()
                await send_file(filename, writer)
            else:
                break

    except Exception as e:
        logging.error(e)

    finally:
        logging.debug('ending connection')
        writer.close()
        await writer.wait_closed()



def path_connection(path: Path) -> Callable[[aio.StreamReader, aio.StreamReader], Awaitable]:
    """Creates a server connection, and specifying the path for the server directory"""
    return lambda reader, writer: server_connection(reader, writer, path)



async def send_file(filename: str, writer: aio.StreamWriter):
    """Used by server side to send file contents to a given client"""
    logging.debug(f'trying to send file {filename}')

    with open(filename, 'rb') as f:
        checksum = hashlib.md5()
        for line in f:
            checksum.update(line)
        checksum = checksum.hexdigest()

        logging.info(f'checksum of: {checksum}')

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
    host = gethostname() # should be loopback
    server = await aio.start_server(path_connection(path), host=host, port=port)
    async with server:
        addr = server.sockets[0].getsockname()
        logging.debug(f'created server on {addr}, listening for clients')
        # todo: better way to stop server
        await server.serve_forever()
 


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    args = argparse.ArgumentParser("creates a server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--dir", default='', help="the directory to where the server hosts files")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")
    # args.add_argument("-s", "--server", action="store_true", help="create a server, will default to a client connection")
    args = args.parse_args()

    # todo: account for config
    path = Path(f'./{args.dir}') # ensure relative path
    aio.run(start_stream(args.port, path))
