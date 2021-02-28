from typing import Callable, Awaitable
from argparse import ArgumentParser
from pathlib import Path

import socket
import asyncio as aio
import hashlib
import logging

import serverdefs as defs


SERVER_PROMPT = """\
1. List files on server
2. Kill Server (any value besides 1 also works)
Select an Option: """


async def server_session(direct: Path):
    """Cli for the server"""
    while option := await defs.ainput(SERVER_PROMPT):
        option = option.rstrip()
        
        if option == '1':
            file_list = '\n'.join(
                d.name for d in direct.iterdir() if d.is_file()
            )
            print('The files on the server:')
            print(f'{file_list}\n')
        else:
            print('Exiting server')
            break



async def server_connection(path: Path, reader: aio.StreamReader, writer: aio.StreamWriter):
    """Callback called for the server wheneven a connection is established with a client"""
    addr, _, _, _ = writer.get_extra_info('peername')
    remote = socket.gethostbyaddr(addr)

    logger = logging.getLogger(remote[0])
    default_logger(logger)
    logger.debug(f"connected to {remote}")

    try:
        pair = defs.StreamPair(reader, writer)
        while request := await reader.readline():
            request = request.decode().strip()

            if request == defs.GET_FILES:
                await list_dir(path, writer, logger)

            elif request == defs.DOWNLOAD:
                await send_file_loop(path, pair, logger)

            else:
                break

    except EOFError:
        pass

    except IOError as e:
        logger.error(e)
        writer.write(str(e).encode())
        writer.write_eof()
        await writer.drain()

    finally:
        logger.debug('ending connection')
        writer.close()
        await writer.wait_closed()



def path_connection(path: Path) -> Callable[[aio.StreamReader, aio.StreamReader], Awaitable]:
    """Creates a stream callback, and specifying the path for the server directory"""
    return lambda reader, writer: server_connection(path, reader, writer)


async def send_file_loop(path: Path, pair: defs.StreamPair, logger: logging.Logger):
    """Runs multiple attempts to send a file based on the receiver response"""
    logger.info("waiting for selected file")
    reader, writer = pair
    filename = await reader.readuntil()
    filename = filename.decode().rstrip()
    filename = f'{path.name}/{filename}'

    tot_bytes = 0
    should_send = True

    while should_send:
        await send_file(filename, writer, logger)
        amnt_read = await reader.readline()
        amnt_read = int(amnt_read.decode().strip())
        tot_bytes += amnt_read

        success = await reader.readline()
        should_send = success.decode().strip() != defs.SUCCESS

    elapsed = await reader.readline()
    elapsed = float(elapsed.decode().strip())
    logger.debug(f'transfer of {filename}: {(tot_bytes/1000):.2f} KB in {elapsed:.5f} secs')



async def send_file(filepath: str, writer: aio.StreamWriter, logger: logging.Logger):
    """Used by server side to send file contents to a given client"""
    logger.debug(f'trying to send file {filepath}')

    with open(filepath, 'rb') as f:
        checksum = hashlib.md5()
        for line in f:
            checksum.update(line)
        checksum = checksum.hexdigest()

        logger.info(f'checksum of: {checksum}')
        writer.write(f'{checksum}\n'.encode())
        await writer.drain()

        f.seek(0)
        writer.writelines(f) # don't need to encode
        await writer.drain()



async def list_dir(direct: Path, writer: aio.StreamWriter, log: logging.Logger):
    """Sends the server directory files to the client"""
    log.info("listing dir")
    file_list = '\n'.join(d.name for d in direct.iterdir() if d.is_file())
    writer.write(file_list.encode())
    await writer.drain()



def init_log(log: str):
    """Specifies logging format and location"""
    log = Path(f'./{log}')
    log_settings = {'format': "%(name)s:%(levelname)s: %(message)s", 'level': logging.DEBUG}

    if not log.exists() or log.is_file():
        logging.basicConfig(filename=log, **log_settings)
    else: # just use stdout
        logging.basicConfig(**log_settings)



async def start_server(port: int, directory: str, log: str, *args, **kwargs):
    """Switch to convert the command args to a given server or client"""
    host = socket.gethostname() # should be loopback

    path = Path(f'./{directory}') # ensure relative path
    path.mkdir(exist_ok=True)

    init_log(log)
    logger = default_logger(logging.getLogger("server"))

    server = await aio.start_server(path_connection(path), host=host, port=port)
    async with server:
        addr = server.sockets[0].getsockname()
        logger.info(f'created server on {addr}, listening for clients')

        aio.create_task(server.serve_forever())
        await aio.create_task(server_session(path))

    await server.wait_closed()
    logger.info("server has stopped")
 


def default_logger(log: logging.Logger):
    """Settings for all created loggers"""
    log.setLevel(logging.DEBUG)
    return log


if __name__ == "__main__":
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser("creates a server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--directory", default='', help="the directory to where the server hosts files")
    args.add_argument("-l", "--log", default='server.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")

    args = args.parse_args()
    args = defs.merge_config_args(args)

    aio.run( start_server(**args) )
