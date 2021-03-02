from json.decoder import JSONDecodeError
from typing import Dict, List
from argparse import ArgumentParser
from pathlib import Path

import sys
import shutil
import json

import asyncio as aio
import asyncio.subprocess as proc
import shlex


PYTHON_CMD = "python" if sys.platform.startswith('win') else "python3"

CONFIGS = 'configs'
SERVERS = 'servers'
CLIENTS = 'clients'
LOGS = 'logs'
TIMES = 'times'


async def start_server(file_size: str, server_id: int) -> proc.Process:
    """
    Launches the server on a separate process with the specified log and directory
    """
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
    """ Creates a number of client processes """
    clients: List[proc.Process] = []

    for i in range(num_clients):
        client_dir = f'{CLIENTS}/{i}'

        if Path(client_dir).exists():
            shutil.rmtree(client_dir)

        client = await aio.create_subprocess_exec(
            *shlex.split(f"{PYTHON_CMD} client.py -c {CONFIGS}/eval-client.ini -d {client_dir} -u client-{i} -v 30"),
            stdin=proc.PIPE
        )

        clients.append(client)

    return clients


async def run_downloads(clients: List[proc.Process]):
    """ Passes input to the client processes to request downloads from the server """
    all_files_in = "1\n\n" * 9 # should be 10 files on each server
    client_input = f'2\n{all_files_in}\n'.encode()
    await aio.gather(*[
        c.communicate(client_input) for c in clients
    ])


async def stop_procs(server: proc.Process, clients: List[proc.Process]):
    """ Sends input to all the procs that should make them cleanly exit """
    await aio.gather(*[
        c.communicate('\n'.encode()) for c in clients
    ])
    await aio.gather(*[c.wait() for c in clients])
    
    await server.communicate('\n'.encode())
    await server.wait()



async def run_cycle(num_clients: int, file_size: str, repeat: int, time_file: str):
    """ Manages the creation, running, and killing of server and client procs """
    for i in range(repeat):
        try:
            server = await start_server(file_size, i)
            clients = await create_clients(num_clients)

            await run_downloads(clients)
            await stop_procs(server, clients)

        except Exception as e:
            print(f'got error {e}')

    times = read_download_times()
    record_times(
        time_file,
        f'{num_clients} clients, {file_size} byte files',
        times
    )

    print('finished cycles')



def read_download_times() -> List[float]:
    """ Parse log files to extract client download times """
    times: List[float] = []

    for log in Path(LOGS).iterdir():
        if not log.is_file():
            continue

        with open(log) as f:
            for line in f:
                if not line.rstrip().endswith('secs'):
                    continue

                toks = line.split()
                times.append(float(toks[-2]))

    return times



def record_times(time_file: str, run_label: str, times: List[float]):
    """ Writes or modifies and existing json file with new time data """
    mode = 'r+' if Path(time_file).exists() else 'w+'
    with open(f'{TIMES}/{time_file}', mode) as f:
        try:
            obj = json.load(f)
        except JSONDecodeError:
            obj = {}

        obj[run_label] = times

        # overwrite content
        f.seek(0)
        json.dump(obj, f)
        f.truncate()



def record_avgs(time_file: str, out_file: str=None):
    """
    Truncates the recorded times to an average. One issue with this view is that the 
    quanitty between different run configs is lost
    """
    if not out_file:
        out_file = time_file

    with open(f'{TIMES}/{time_file}', 'r') as r:
        file_times: Dict[str, List[float]] = json.load(r)
        avgs = {
            label: sum(times) / len(times)
            for label, times in file_times.items()
        }

    with open(f'{TIMES}/{out_file}', 'w') as w:
        json.dump(avgs, w)



if __name__ == "__main__":
    if sys.version_info < (3, 8):
        raise RuntimeError("Python version needs to be at least 3.8")

    args = ArgumentParser("Runs various configurations for server client set ups")
    args.add_argument("-n", "--num_clients", type=int, default=4, help="the number of concurrent clients")
    args.add_argument("-f", "--file_size", choices=['128', '512', '2k', '8k', '32k'], default='128', help="the size of each file downloaded")
    args.add_argument("-j", "--json", default="times.json", help="the json file where the times will be recorded")
    args.add_argument("-r", "--repeat", type=int, default=2, help="the amount of repeated runs")
    args = args.parse_args()

    aio.run(run_cycle(
        args.num_clients,
        args.file_size,
        args.repeat,
        args.json
    ))
