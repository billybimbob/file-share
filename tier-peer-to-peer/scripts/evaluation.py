#!/usr/bin/env python3

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, NamedTuple, Optional, Union

from argparse import ArgumentParser
from pathlib import Path

import asyncio as aio
import asyncio.subprocess as proc
import shlex

import json
import os
import random
import shutil
import sys


#region directory locations

CONFIGS = 'configs'
SERVERS = 'servers'
PEERS = 'peers'
LOGS = 'logs'

#endregion

CHUNK_SIZE = 1024
PEER_PORT_BASE = 9989

label: str

#region dry run peers

class PeerRun:
    """
    Ran peer process, and the initial files from the peer directory before
    it was ran
    """
    process: Optional[proc.Process]
    dir: Path
    start_files: frozenset[Path]

    def __init__(
        self,
        dir: Union[str, Path],
        process: Optional[proc.Process] = None,
        starts: Optional[Iterable[str]] = None):

        self.process = process
        self.dir = dir if isinstance(dir, Path) else Path(dir)

        if starts:
            self.start_files = frozenset(
                start_path
                for start in starts
                if (start_path := self.dir.joinpath(start)).exists() )
        else:
            self.start_files = frozenset(self.dir.iterdir())


def dry_run_peers(start_loc: str, **kwargs: Any):
    " Creates or resets directory structure with processes "
    start_files: Optional[dict[str, list[str]]] = None
    if start_loc:
        try:
            with open(start_loc, "r+") as f:
                start_files = json.load(f)
        except Exception:
            pass

    if start_files is None: # create and store starting names
        write_to = start_loc if start_loc else f'{PEERS}/starters'
        write_to = Path(write_to).with_suffix('.json')

        start_files = dry_create_peers(**kwargs)

        with open(write_to, 'w+') as f:
            json.dump(start_files, f)
        
    else: # reset directory
        peers = [
            PeerRun(dir_name, starts=starts)
            for dir_name, starts in start_files.items()
        ]
        reset_peers_path(peers)



def dry_create_peers(num_peers: int, file_size: str) -> dict[str, list[str]]:
    " Sets up peer directory structure without creating the processes "
    src_dir = f'{SERVERS}/{file_size}'

    peers = list[PeerRun]()
    for i in range(num_peers):
        user = f'peer{i}'
        log = f'{LOGS}/{label}/{user}.log'
        peer_dir = f'{PEERS}/{Path(src_dir).name}/{i}'

        init_peer_paths(user, log, peer_dir, src_dir)
        peers.append(PeerRun(peer_dir))

    return {
        str(p.dir): [f.name for f in p.start_files]
        for p in peers
    }

#endregion


#region weak peers

async def create_peers(
    supers: Sequence[SuperRun],
    num_peers: int,
    file_size: str,
    verbosity: int) -> Sequence[PeerRun]:
    """ Creates a number of client processes """

    src_dir = f'{SERVERS}/{file_size}'

    async def get_super(id: int):
        """ Selects a super peer port and init """
        sup_port = supers[id % len(supers)].port
        return await init_peer(sup_port, id, verbosity, src_dir)

    peers = await aio.gather(*[
        get_super(i) for i in range(num_peers)
    ])

    return peers


async def init_peer(
    sup_port: int, id: int, verbosity: int, src_dir: str) -> PeerRun:
    """
    Starts a peer process, and also waits for a peer response, which should 
    indicate that the peer is ready to take in user input
    """
    user = f'peer{id}'
    log = f'{LOGS}/{label}/{user}.log'
    peer_dir = f'{PEERS}/{Path(src_dir).name}/{id}'

    init_peer_paths(user, log, peer_dir, src_dir)

    peer = await aio.create_subprocess_exec(
        *shlex.split(
            f"./weakpeer.py -p {PEER_PORT_BASE + id} -s {sup_port} "
            f"-d \"{peer_dir}\" -u \"{user}\" -l \"{log}\" -v {verbosity}"
        ),
        stdin=proc.PIPE,
        stdout=proc.PIPE
    )

    if peer.stdin:
        try:
            peer.stdin.write('1\n'.encode())
            await aio.wait_for(peer.stdin.drain(), timeout=5)
        except (aio.CancelledError, aio.TimeoutError):
            pass

        await get_response(peer)

    return PeerRun(peer_dir, peer)


def init_peer_paths(user: str, log: str, peer_dir: str, src_dir: str):
    """ Inits log info and sets up peer directory """
    if not ((logpath := Path(log)).exists()):
        logpath.parent.mkdir(exist_ok=True, parents=True)

    # if not (peer_path := Path(peer_dir)).exists():
    peer_path = Path(peer_dir)
    if not peer_path.exists():
        peer_path.mkdir(parents=True)
        shutil.copytree(src_dir, peer_dir, dirs_exist_ok=True)

        old_names = list(peer_path.iterdir())
        new_names = [
            old.with_name(f'{old.stem}-{user}{"".join(old.suffixes)}')
            for old in old_names
        ]

        # rename files
        for old, new in zip(old_names, new_names):
            shutil.move(str(old), str(new))


def reset_peers_path(peers: Iterable[PeerRun]):
    """ Removes all files that are not in a peer directory start files set """
    # remove downloaded files during run
    for p in peers:
        # shutil.rmtree(p.dir)
        new_files = set(p.dir.iterdir())
        new_files.difference_update(p.start_files)
        for new in new_files:
            if new.is_dir():
                shutil.rmtree(new)
                new.rmdir()
            else:
                os.remove(new)


async def run_downloads(
    peers: Sequence[PeerRun], num_queries: int, tot_requests: int):
    """
    Passes input to the client processes to request downloads from the server
    """
    # does not account for the case where the are no files to download
    # should not happen
    if num_queries <= 0:
        return
    elif num_queries >= len(peers):
        raise ValueError('too many query peers specified')

    rand_peers = [
        random.randint(0, len(peers)-1)
        for _ in range(num_queries)
    ]

    min_amt = min(len(p.start_files) for p in peers)
    req_range = min_amt * len(peers)

    async def request_peer(peer: PeerRun, run: int):
        if peer.process is None:
            print('no process was given')
            return 

        if peer.process.stdin is None:
            print('peer stdin is None')
            return

        try:
            num_requests = (
                tot_requests // num_queries
                if run < num_queries else
                tot_requests // num_queries + tot_requests % num_queries)

            client_input = ( # not great, missing the bottom ones
                f'2\n{random.randint(0, req_range)}\n'
                for _ in range(num_requests))
            client_input = ''.join(client_input)
            client_input = client_input.encode()

            peer.process.stdin.write(client_input)
            await aio.wait_for(peer.process.stdin.drain(), timeout=5)

        except (aio.CancelledError, aio.TimeoutError):
            pass

        await get_response(peer.process)

    await aio.gather(*[
        request_peer(peers[idx], i+1)
        for i, idx in enumerate(rand_peers)
    ])


async def interact_peer(
    sup_port: int, num_peers: int, file_size: str, verbosity: int):
    """
    Creates an extra peer that will connect to the given super that can be
    interfaced with the terminal
    """
    user = 'interact'
    log = f'{LOGS}/{label}/{user}.log'
    peer_dir = f'{PEERS}/{file_size}/{user}'

    peer_path = Path(f'./{peer_dir}')
    shutil.rmtree(peer_path)
    peer_path.mkdir(parents=True)

    peer = await aio.create_subprocess_exec(
        *shlex.split(
            f"./weakpeer.py -p {PEER_PORT_BASE + num_peers + 1} -s {sup_port} "
            f"-d \"{peer_dir}\" -u \"{user}\" -l \"{log}\" -v {verbosity}"
        )
    )

    await peer.wait()


async def stop_peers(peers: Iterable[PeerRun]):
    """ Sends input to all the clients that should make them cleanly exit """
    async def stop(peer: PeerRun):
        """ Checked stop communication """
        if peer.process is None:
            print('process is None')
        else:
            await peer.process.communicate('\n'.encode())

    await aio.gather(*[ stop(p) for p in peers ])
    reset_peers_path(peers)

#endregion



async def get_response(peer: proc.Process):
    """ Tries to block until stdout has no more output """
    if not peer.stdout:
        print('cannot get response from peer')
        return

    try:
        while True:
            # might want to make wait interval as param
            await aio.wait_for(peer.stdout.read(CHUNK_SIZE), timeout=5)

    except (aio.TimeoutError, aio.CancelledError):
        pass



#region super peers

class SuperRun(NamedTuple):
    port: int
    process: proc.Process


async def start_supers(ports: Iterable[int], map: str) -> Sequence[SuperRun]:
    """
    Launches the server on a separate process with the specified log and directory
    """
    servers = list[SuperRun]()
    for i, port in enumerate(ports):
        sup = f'super{i}'
        log = f'{LOGS}/{label}/{sup}.log'

        # clear log content
        if (Path(f'./{log}').exists()):
            open(log, 'w').close()

        server = await aio.create_subprocess_exec(
            *shlex.split(f"./superpeer.py -p {port} -l \"{log}\" -m {map}"),
            stdin=proc.PIPE,
            stdout=proc.PIPE
        )

        await get_response(server)
        servers.append(SuperRun(port, server))

    return servers


async def stop_supers(supers: Iterable[SuperRun]):
    """ Sends input to all the server that should make it cleanly exit """
    await aio.gather(*[
        sup.process.communicate('\n'.encode())
        for sup in supers
    ])

#endregion


async def run_cycle(
    map: str,
    num_peers: int,
    file_size: str,
    query_peers: int,
    requests: int,
    verbosity: int,
    interactive: bool,
    dry_run: Optional[str]):
    """ Manages the creation, running, and killing of server and client procs """

    if dry_run is not None:
        dry_run_peers(dry_run, num_peers=num_peers, file_size=file_size)
        return

    global label
    map_path = Path(f'./{map}')
    label = f'{map_path.stem}{num_peers}w{query_peers}q'
    ports = parse_ports(map)

    supers = await start_supers(ports, map)
    peers = await create_peers(supers, num_peers, file_size, verbosity)

    queries = [ run_downloads(peers, query_peers, requests) ]
    if interactive:
        queries.append(
            interact_peer(supers[0].port, num_peers, file_size, verbosity))

    await aio.gather(*queries)

    await stop_peers(peers)
    await stop_supers(supers)



def parse_ports(map: Union[Path, str], **_) -> Iterable[int]:
    """ Get all the ports as args in the given topology """
    if isinstance(map, str):
        map = Path(f'./{map}')

    map = map.with_suffix('.json')
    with open(map) as f:
        items: list[dict[str, Any]] = json.load(f)
        return [item['port'] for item in items]


if __name__ == "__main__":
    if sys.version_info < (3,9):
        raise RuntimeError("Python version needs to be at least 3.9")

    args = ArgumentParser(description="Runs various configurations for peer clients")
    args.add_argument("-d", "--dry-run", nargs="?", default=None, const='', help="run peer init without starting the processes; file path can be given to reset dir files")
    args.add_argument("-f", "--file-size", default='128', choices=['128', '512', '2k', '8k', '32k'], help="the size of each file downloaded")
    args.add_argument("-i", "--interactive", action='store_true', help="creates an extra interactive peer to interface with the system")
    args.add_argument("-m", "--map", required=True, help="the super network topology as a json path")
    args.add_argument("-n", "--num-peers", type=int, default=2, help="the number of concurrent clients")
    args.add_argument("-q", "--query-peers", type=int, default=1, help="the amount of query peers")
    args.add_argument("-r", "--requests", type=int, default=10, help="total amount of query requests")
    args.add_argument("-v", "--verbosity", type=int, default=10, choices=[0, 10, 20, 30, 40, 50], help="the logging verboseness, level corresponds to default levels")
    args = args.parse_args()

    aio.run(run_cycle(**vars(args)))
