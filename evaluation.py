from typing import List
from argparse import ArgumentParser
from pathlib import Path

import sys
import asyncio as aio
import asyncio.subprocess as proc
import shlex

PYTHON_CMD = "python" if sys.platform.startswith('win') else "python3"
SERVERS = 'servers'


async def start_server(file_size: str, log: str) -> proc.Process:
    server_dir = f'{SERVERS}/{file_size}'
    server = await aio.create_subprocess_exec(
        *shlex.split(f"{PYTHON_CMD} server.py -c configs/eval-server.ini -d {server_dir} -l {log}"),
        stdin=proc.PIPE
    )

    return server




async def create_clients(num_clients: int) -> List[proc.Process]:
    clients: List[proc.Process] = []
    for i in range(num_clients):
        client = await aio.create_subprocess_exec(
            *shlex.split(f"{PYTHON_CMD} client.py -c configs/eval-client.ini -d clients/{i}"),
            stdin=proc.PIPE
        )
        clients.append(client)

    return clients


async def run_downloads(clients: List[proc.Process]):
    all_files_in = "1\n\n" * 9
    client_input = f'2\n{all_files_in}\n'.encode() # should be 10 files on each server
    await aio.gather(*[
        c.communicate(client_input) for c in clients
    ])


async def stop_procs(server: proc.Process, clients: List[proc.Process]):
    await aio.gather(*[
        c.communicate('\n'.encode()) for c in clients
    ])
    await server.communicate('\n'.encode())


async def run_cycle(num_clients: int, file_size: str, repeat: int):
    for i in range(repeat):
        server = await start_server(file_size, f'logs/{i}.log')
        clients = await create_clients(num_clients)

        await stop_procs(server, clients)


if __name__ == "__main__":
    if sys.version_info < (3, 8):
        raise RuntimeError("Python version needs to be at least 3.8")

    args = ArgumentParser("Runs various configurations for server client set ups")
    args.add_argument("-n", "--num_clients", type=int, default=4, help="the number of concurrent clients")
    args.add_argument("-f", "--file_size", choices=['128', '512', '2k', '8k', '32k'], default='128', help="the size of each file downloaded")
    args.add_argument("-r", "--repeat", type=int, default=2, help="the amount of repeated runs")
    args = args.parse_args()

    aio.run(run_cycle(
        args.num_clients,
        args.file_size,
        args.repeat
    ))
