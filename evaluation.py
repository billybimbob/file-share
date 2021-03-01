from typing import List
from argparse import ArgumentParser
from pathlib import Path

import sys
import asyncio as aio
import shlex

PYTHON_CMD = "python" if sys.platform.startswith('win') else "python3"
SERVERS = 'servers'


async def start_server(file_size: str, log: str) -> aio.Process:
    server_dir = f'{SERVERS}/{file_size}'
    server = await aio.create_subprocess_exec(
        *shlex.split(f"{PYTHON_CMD} server.py -c configs/eval-server.ini -d {server_dir} -l {log}"),
        stdin=aio.subprocess.PIPE
    )

    return server

async def stop_server(server: aio.Process):
    await server.communicate('\n'.encode())


async def create_clients(num_clients: int) -> List[aio.Process]:
    clients: List[aio.Process] = []
    for i in num_clients:
        client = await aio.create_subprocess_exec(
            *shlex.split(f"{PYTHON_CMD} client.py -c configs/eval-client.ini -d clients/{i}")
        )
        clients.append(client)

    return clients


async def stop_clients(clients: List[aio.Process]):
    await aio.gather(*[
        c.communicate('\n'.encode) for c in clients
    ])


if __name__ == "__main__":
    if sys.version_info < (3, 8):
        raise RuntimeError("Python version needs to be at least 3.8")

    args = ArgumentParser("Runs various configurations for server client set ups")
    args.add_argument("-n", "--num-clients", type=int, default=4, help="the number of concurrent clients")
    args.add_argument("-f", "--file-size", choices=['128', '512', '2k', '8k', '32k'], default='128', help="the size of each file downloaded")
    args.add_argument("-r", "repeat", type=int, default=2, help="the amount of repeated runs")
    args = args.parse_args()
