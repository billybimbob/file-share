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
# sys.executable not working?


# region directory locations

CONFIGS = 'configs'
SERVERS = 'servers'
CLIENTS = 'clients'

LOGS = 'logs'
TIMES = 'times'

# endregion


async def start_server(file_size: str, log: str) -> proc.Process:
    """
    Launches the server on a separate process with the specified log and directory
    """
    server_dir = f'{SERVERS}/{file_size}'

    # clear log content
    if (Path(f'./{log}').exists()):
        open(log, 'w').close()

    server = await aio.create_subprocess_exec(
        *shlex.split(f"{PYTHON_CMD} server.py -c {CONFIGS}/eval-server.ini -d {server_dir} -l {log}"),
        stdin=proc.PIPE
    )

    return server


async def stop_server(server: proc.Process):
    """ Sends input to all the server that should make it cleanly exit """
    await server.communicate('\n'.encode())
    await server.wait()



async def create_clients(num_clients: int, label: str, verbosity: int) -> List[proc.Process]:
    """ Creates a number of client processes """
    clients: List[proc.Process] = []

    for i in range(num_clients):
        client_dir = f'{CLIENTS}/{i}'
        log = f'{LOGS}/clients/client{i}-{label}.log'

        if ((logpath := Path(log)).exists()):
            # clear log content
            open(log, 'w').close()
        else:
            logpath.parent.mkdir(exist_ok=True, parents=True)

        if Path(client_dir).exists():
            shutil.rmtree(client_dir)

        client = await aio.create_subprocess_exec(
            *shlex.split(
                f"{PYTHON_CMD} client.py -c {CONFIGS}/eval-client.ini "
                f"-d {client_dir} -u client-{i} -l {log} -v {verbosity}"
            ),
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


async def stop_clients(clients: List[proc.Process]):
    """ Sends input to all the clients that should make them cleanly exit """
    await aio.gather(*[
        c.communicate('\n'.encode()) for c in clients
    ])
    await aio.gather(*[c.wait() for c in clients])



async def run_cycle(num_clients: int, file_size: str, repeat: int, time_file: str, verbosity: int):
    """ Manages the creation, running, and killing of server and client procs """

    run_label = f'{num_clients}c{file_size}f'
    server_log = f'{LOGS}/server-{run_label}.log'
    server = await start_server(file_size, server_log)


    for _ in range(repeat):
        clients = await create_clients(num_clients, run_label, verbosity)
        await run_downloads(clients)
        await stop_clients(clients)
    
    await stop_server(server)

    times = read_download_times(server_log)
    record_times(
        time_file,
        f'{num_clients} clients, {file_size} byte files',
        times
    )

    print('finished cycles')



def read_download_times(log: str) -> List[float]:
    """ Parse log files to extract client download times """
    times: List[float] = []
    with open(log) as f:
        for line in f:
            if not line.rstrip().endswith('secs'):
                continue

            toks = line.split()
            times.append(float(toks[-2]))

    return times



def record_times(time_file: str, run_label: str, times: List[float]):
    """ Writes or modifies and existing json file with new time data """
    filepath = Path(f'{TIMES}/{time_file}').with_suffix(".json")
    mode = 'r+' if filepath.exists() else 'w+'

    with open(filepath, mode) as f:
        try:
            obj = json.load(f)
        except json.JSONDecodeError:
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
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")
    args = args.parse_args()

    aio.run(run_cycle(
        args.num_clients,
        args.file_size,
        args.repeat,
        args.json,
        args.verbosity
    ))
