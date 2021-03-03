from typing import Any, Callable, Coroutine, cast
from argparse import ArgumentParser
from pathlib import Path

import socket
import asyncio as aio
import hashlib
import logging

from server_defs import (
    Message, StreamPair,
    GET_FILES, DOWNLOAD, SUCCESS,
    ainput, merge_config_args, version_check
)


SERVER_PROMPT = """\
1. List files on server
2. Kill Server (any value besides 1 also works)
Select an Option: """


async def server_session(direct: Path):
    """ Cli for the server """
    while option := await ainput(SERVER_PROMPT):
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



async def server_connection(path: Path, pair: StreamPair):
    """
    Callback called for the server wheneven a connection is established
    with a client
    """
    reader, writer = pair 
    addr, _, _, _ = writer.get_extra_info('peername')
    remote = socket.gethostbyaddr(addr)

    # username = remote[0]
    username = await Message.read(reader)

    logger = logging.getLogger(username)
    logger = default_logger(logger)
    logger.debug(f"connected to {username}: {remote}")

    try:
        while request := await Message.read(reader):

            if request == GET_FILES:
                await list_dir(path, writer, logger)

            elif request == DOWNLOAD:
                await send_file_loop(path, pair, logger)

            else:
                break

    except EOFError:
        pass
    except IOError as e:
        logger.error(e)
        await Message.write(writer, str(e), error=True)

    finally:
        logger.debug('ending connection')
        writer.write_eof()
        writer.close()
        await writer.wait_closed()



def path_connection(path: Path) \
    -> Callable[[aio.StreamReader, aio.StreamWriter], Coroutine[Any, Any, None]]:
    """
    Creates a stream callback, and specifying the path for the server directory
    """
    return lambda reader, writer: \
        server_connection(path, StreamPair(reader, writer))



async def list_dir(direct: Path, writer: aio.StreamWriter, log: logging.Logger):
    """ Sends the server directory files to the client """
    log.info("listing dir")
    file_list = '\n'.join(d.name for d in direct.iterdir() if d.is_file())
    await Message.write(writer, file_list)



async def send_file_loop(
    path: Path, pair: StreamPair, logger: logging.Logger):
    """
    Runs multiple attempts to send a file based on the receiver response
    """
    logger.info("waiting for selected file")
    reader, writer = pair

    filename = await Message.read(reader)
    filepath = path.joinpath(filename)

    tot_bytes = 0
    should_send = True

    while should_send:
        await send_file(filepath, writer, logger)

        amt_read = await Message.read(reader)
        tot_bytes += int(amt_read)

        success = await Message.read(reader)
        should_send = success != SUCCESS

    elapsed = await Message.read(reader)
    elapsed = float(elapsed)
    logger.debug(
        f'transfer of {filename}: {(tot_bytes/1000):.2f} KB in {elapsed:.5f} secs'
    )



async def send_file(filepath: Path, writer: aio.StreamWriter, logger: logging.Logger):
    """ Used by server side to send file contents to a given client """
    logger.debug(f'trying to send file {filepath}')

    with open(filepath, 'rb') as f:
        checksum = hashlib.md5()
        for line in f:
            checksum.update(line)
        checksum = checksum.digest()

        # logger.info(f'checksum of: {checksum}')
        await Message.write(writer, checksum)

        filesize = filepath.stat().st_size
        await Message.write(writer, str(filesize))

        f.seek(0)
        writer.writelines(f) # don't need to encode
        await writer.drain()



def init_log(log: str):
    """ Specifies logging format and location """
    log_path = Path(f'./{log}')
    log_path.parent.mkdir(exist_ok=True, parents=True)
    log_settings = {'format': "%(levelname)s: %(name)s: %(message)s", 'level': logging.DEBUG}

    if not log_path.exists() or log_path.is_file():
        logging.basicConfig(filename=log, **log_settings)
    else: # just use stdout
        logging.basicConfig(**log_settings)



async def start_server(port: int, directory: str, log: str, *args, **kwargs):
    """ Switch to convert the command args to a given server or client """
    host = socket.gethostname() # should be loopback

    path = Path(f'./{directory}') # ensure relative path
    path.mkdir(exist_ok=True, parents=True)

    init_log(log)
    logger = default_logger(logging.getLogger("server"))

    server = await aio.start_server(path_connection(path), host=host, port=port)
    async with server:
        addr: str = server.sockets[0].getsockname()
        logger.info(f'created server on {addr}, listening for clients')

        aio.create_task(server.serve_forever())
        await aio.create_task(server_session(path))

    await server.wait_closed()
    logger.info("server has stopped")
 


def default_logger(log: logging.Logger) -> logging.Logger:
    """ Settings for all created loggers """
    log.setLevel(logging.DEBUG)
    return log


if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser("creates a server")
    args.add_argument("-c", "--config", help="base arguments on a config file, other args will be ignored")
    args.add_argument("-d", "--directory", default='', help="the directory to where the server hosts files")
    args.add_argument("-l", "--log", default='server.log', help="the file to write log info to")
    args.add_argument("-p", "--port", type=int, default=8888, help="the port to run the server on")

    args = args.parse_args()
    args = merge_config_args(args)

    aio.run( start_server(**args) )
