#!/usr/bin/env python3

from __future__ import annotations
from collections.abc import Sequence
from connection import CHUNK_SIZE

from argparse import ArgumentParser
from pathlib import Path

import sys
import shutil

import asyncio as aio
import asyncio.subprocess as proc
import shlex


# region directory locations

CONFIGS = 'configs'
SERVERS = 'servers'
CLIENTS = 'clients' # TODO: change to peers
LOGS = 'logs'

# endregion


async def start_indexer(log: str) -> proc.Process:
    """
    Launches the server on a separate process with the specified log and directory
    """
    # clear log content
    if (Path(f'./{log}').exists()):
        open(log, 'w').close()

    server = await aio.create_subprocess_exec(
        *shlex.split(f"./indexer.py -c {CONFIGS}/eval-server.ini -l {log}"),
        stdin=proc.PIPE
    )

    return server


async def stop_indexer(indexer: proc.Process):
    """ Sends input to all the server that should make it cleanly exit """
    await indexer.communicate('\n'.encode())



async def create_peers(num_peers: int, label: str, verbosity: int) -> Sequence[proc.Process]:
    """ Creates a number of client processes """
    return await aio.gather(*[
        init_peer(label, i, verbosity)
        for i in range(num_peers)
    ])


async def init_peer(label: str, id: int, verbosity: int) -> proc.Process:
    """
    Starts a peer process, and also waits for a peer response, which should 
    indicate that the peer is ready to take in user input
    """
    # TODO: change to peer dir structure and fix args
    peer_dir = f'{CLIENTS}/{id}'
    log = f'{LOGS}/clients/client{id}-{label}.log'
    
    if ((logpath := Path(log)).exists()):
        open(log, 'w').close()
    else:
        logpath.parent.mkdir(exist_ok=True, parents=True)

    if Path(peer_dir).exists():
        shutil.rmtree(peer_dir)

    peer = await aio.create_subprocess_exec(
        *shlex.split(
            f"./peer.py -c {CONFIGS}/eval-client.ini "
            f"-d {peer_dir} -u peer-{id} -l {log} -v {verbosity}"
        ),
        stdin=proc.PIPE,
        stdout=proc.PIPE
    )

    if not peer.stdin or not peer.stdout:
        print(f'peer {id} failed to init stdin')
        return peer

    peer.stdin.write('1\n'.encode())
    await peer.stdin.drain()

    try:
        read = 0
        while read == 0 or read == CHUNK_SIZE:
            # might want to make wait interval as param
            read = await aio.wait_for(peer.stdout.read(CHUNK_SIZE), 5)
            read = len(read)

    except aio.TimeoutError:
        pass

    return peer


async def run_downloads(clients: Sequence[proc.Process]):
    """ Passes input to the client processes to request downloads from the server """
    # TODO: modify to use peers and to not use communicate
    all_files_in = "1\n\n" * 9 # should be 10 files on each server
    client_input = f'2\n{all_files_in}\n'.encode()
    await aio.gather(*[
        c.communicate(client_input) for c in clients
    ])


async def stop_peers(peers: Sequence[proc.Process]):
    """ Sends input to all the clients that should make them cleanly exit """
    await aio.gather(*[
        p.communicate('\n'.encode()) for p in peers
    ])



async def run_cycle(num_peers: int, file_size: str, repeat: int, verbosity: int):
    """ Manages the creation, running, and killing of server and client procs """

    run_label = f'{num_peers}c{file_size}f'
    indexer_log = f'{LOGS}/indexer-{run_label}.log'
    indexer = await start_indexer(indexer_log)

    for _ in range(repeat):
        peers = await create_peers(num_peers, run_label, verbosity)
        await run_downloads(peers)
        await stop_peers(peers)
    
    await stop_indexer(indexer)

    print('finished cycles')



if __name__ == "__main__":
    if sys.version_info < (3, 8):
        raise RuntimeError("Python version needs to be at least 3.8")

    args = ArgumentParser("Runs various configurations for server client set ups")
    args.add_argument("-f", "--file_size", choices=['128', '512', '2k', '8k', '32k'], default='128', help="the size of each file downloaded")
    args.add_argument("-n", "--num_peers", type=int, default=4, help="the number of concurrent clients")
    args.add_argument("-r", "--repeat", type=int, default=2, help="the amount of repeated runs")
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")
    args = args.parse_args()

    aio.run(run_cycle(
        args.num_peers,
        args.file_size,
        args.repeat,
        args.verbosity
    ))
