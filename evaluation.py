from typing import List
from argparse import ArgumentParser
from pathlib import Path

import sys
import shutil

import asyncio as aio
import asyncio.subprocess as proc
import shlex

PYTHON_CMD = "python" if sys.platform.startswith('win') else "python3"

CONFIGS = 'configs'
SERVERS = 'servers'
CLIENTS = 'clients'
LOGS = 'logs'


async def start_server(file_size: str, server_id: int) -> proc.Process:
    server_dir = f'{SERVERS}/{file_size}'
    log = f'{LOGS}/{server_id}.log'

    # clear log content
    if (Path(f'./{log}').exists()):
        open(log, 'w').close()

    server = await aio.create_subprocess_exec(
        *shlex.split(f"{PYTHON_CMD} server.py -c {CONFIGS}/eval-server.ini -d {server_dir} -l {log}"),
        stdin=proc.PIPE
    )

    return server


async def create_clients(num_clients: int) -> List[proc.Process]:
    clients: List[proc.Process] = []

    for i in range(num_clients):
        client_dir = f'{CLIENTS}/{i}'

        if Path(client_dir).exists():
            shutil.rmtree(client_dir)

        client = await aio.create_subprocess_exec(
            *shlex.split(f"{PYTHON_CMD} client.py -c {CONFIGS}/eval-client.ini -d {client_dir}"),
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
    await aio.gather(*[c.wait() for c in clients])
    
    await server.communicate('\n'.encode())
    await server.wait()


async def run_cycle(num_clients: int, file_size: str, repeat: int):
    for i in range(repeat):
        try:
            server = await start_server(file_size, i)
            clients = await create_clients(num_clients)

            await run_downloads(clients)
            await stop_procs(server, clients)

        except Exception as e:
            print(f'got error {e}')

    print('finished cycles')


if __name__ == "__main__":
    if sys.version_info < (3, 8):
        raise RuntimeError("Python version needs to be at least 3.8")

    args = ArgumentParser("Runs various configurations for server client set ups")
    args.add_argument("-n", "--num_clients", type=int, default=4, help="the number of concurrent clients")
    args.add_argument("-f", "--file_size", choices=['128', '512', '2k', '8k', '32k'], default='128', help="the size of each file downloaded")
    args.add_argument("-r", "--repeat", type=int, default=2, help="the amount of repeated runs")
    args = args.parse_args()

    try:
        aio.run(run_cycle(
            args.num_clients,
            args.file_size,
            args.repeat
        ))
    except:
        print('error in run')
