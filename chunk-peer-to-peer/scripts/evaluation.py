#!/usr/bin/env python3

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Optional, Union

from argparse import ArgumentParser
from pathlib import Path

import asyncio as aio
import asyncio.subprocess as proc
import shlex

import json
import os
import random

import shutil
import string
import sys


#region path locations

CONFIGS = Path('configs')
SERVERS = Path('servers')
PEERS = Path('peers')
LOGS = Path('logs')

PEER = Path('peer.py')
INDEXER = Path('indexer.py')

#endregion


CHUNK_SIZE = 1024
PEER_PORT_BASE = 9989

SIZE_MAP = {
    '1m': 1_048_576
}



def init_server_files(file_size: str, min_copies: int):
    """ 
    Standardizes file names for a server, and generates missing files
    if needed
    """
    target_server = SERVERS / file_size
    target_server.mkdir(parents=True, exist_ok=True)

    curr_files = sorted(
        (f for f in target_server.iterdir() if f.is_file()),
        key = lambda f: f.name)

    new_files = [
        f.with_name(
            f'{file_size}({i})').with_suffix(''.join(f.suffixes))
        for i, f in enumerate(curr_files) ]
    
    for old, new in zip(curr_files, new_files):
        if old != new:
            shutil.move(old, new)

    num_files = len(new_files)

    if num_files >= min_copies:
        return

    poss_chars = string.ascii_letters + string.digits + ' \n'

    for i in range(num_files, min_copies):
        new_file = target_server / f'{file_size}({i}).txt'

        with open(new_file, 'x') as f:
            # potential stack issue
            rand_bytes = [
                random.choice(poss_chars)
                for _ in range(SIZE_MAP[file_size]) ]

            f.write(''.join(rand_bytes))


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
                if (start_path := self.dir / start).exists() )
        else:
            self.start_files = frozenset(self.dir.iterdir())


def dry_run_peers(start_loc: str, **kwargs: Any):
    """ Creates or resets directory structure with processes """
    start_files: Optional[dict[str, list[str]]] = None
    if start_loc:
        try:
            with open(start_loc, "r+") as f:
                start_files = json.load(f)

        except Exception:
            pass

    if start_files is None: # create and store starting names
        write_to = (
            Path(start_loc)
            if start_loc else
            PEERS / 'starters')

        write_to = write_to.with_suffix('.json')
        start_files = dry_create_peers(**kwargs)

        with open(write_to, 'w+') as f:
            json.dump(start_files, f)
        
    else: # reset directory
        peers = [
            PeerRun(dir_name, starts=starts)
            for dir_name, starts in start_files.items() ]

        reset_peers_path(peers)



def dry_create_peers(
    label: str, num_peers: int, file_size: str) -> dict[str, list[str]]:
    """ Sets up peer directory structure without creating the processes """
    src_dir = SERVERS.joinpath(file_size)
    peers = list[PeerRun]()

    for i in range(num_peers):
        user = f'peer{i}'
        log = LOGS / label / PEERS / f'{user}.log'
        peer_dir = PEERS / src_dir.name / str(i)

        init_peer_paths(user, log, peer_dir, src_dir)
        peers.append(PeerRun(peer_dir))

    return {
        str(p.dir): [f.name for f in p.start_files]
        for p in peers
    }

#endregion


#region peers

async def create_peers(
    label: str,
    num_peers: int,
    file_size: str,
    verbosity: int) -> Sequence[PeerRun]:
    """ Creates a number of client processes """
    src_dir = SERVERS / file_size
    peers = await aio.gather(*[
        init_peer(label, i, verbosity, src_dir) for i in range(num_peers) ])

    return peers


async def init_peer(label: str, id: int, verbosity: int, src_dir: Path) -> PeerRun:
    """
    Starts a peer process, and also waits for a peer response, which should 
    indicate that the peer is ready to take in user input
    """
    user = f'peer{id}'
    log = LOGS / label / PEERS / f'{user}.log'
    peer_dir = PEERS / src_dir.name / str(id)
    config = CONFIGS / 'eval-peer.ini'

    init_peer_paths(user, log, peer_dir, src_dir)

    peer = await aio.create_subprocess_exec(
        *shlex.split(
            f'./{PEER} -c {config} -p {PEER_PORT_BASE + id} '
            f'-d "{peer_dir}" -u "{user}" -l "{log}" -v {verbosity}'),
        stdin = proc.PIPE,
        stdout = proc.PIPE)

    if peer.stdin:
        peer.stdin.write('1\n'.encode())
        await peer.stdin.drain()
        await get_response(peer)

    return PeerRun(peer_dir, peer)


def init_peer_paths(user: str, log: Path, peer_dir: Path, src_dir: Path):
    """ Inits log info and sets up peer directory """
    if not log.exists():
        log.parent.mkdir(exist_ok=True, parents=True)

    if not peer_dir.exists():
        peer_dir.mkdir(parents=True)
        shutil.copytree(src_dir, peer_dir, dirs_exist_ok=True)

        # don't rename files

        # old_names = list(peer_dir.iterdir())
        # new_names = [
        #     old.with_name(f'{old.stem}-{user}{"".join(old.suffixes)}')
        #     for old in old_names ]

        # # rename files
        # for old, new in zip(old_names, new_names):
        #     shutil.move(str(old), str(new))


def reset_peers_path(peers: Iterable[PeerRun]):
    """ 
    Removes all files that are not in a peer directory start files set
    """
    # remove downloaded files during run
    for p in peers:
        new_files = set(p.dir.iterdir()) - p.start_files
        for new in new_files:
            if new.is_dir():
                shutil.rmtree(new)
                new.rmdir()
            else:
                os.remove(new)


async def run_downloads(
    label: str, num_peers: int, file_size: str, num_requests: int):
    """
    Creates and passes input to the to a new peer processes to 
    request downloads from the server
    """
    user = 'requester'
    log = LOGS / label / PEERS / f'{user}.log'
    peer_dir = PEERS  / file_size / user
    config = CONFIGS / 'eval-peer.ini'

    if peer_dir.exists():
        shutil.rmtree(peer_dir)

    requester = await aio.create_subprocess_exec(
        *shlex.split(
            f'./{PEER} -c {config} -p {PEER_PORT_BASE + num_peers + 1} '
            f'-d "{peer_dir}" -u "{user}" -l "{log}"'),
        stdin = proc.PIPE,
        stdout = proc.PIPE)

    requests = [ f'2\n{i + 1}\n' for i in range(num_requests) ]
    requests = ''.join(requests) + '\n' # last \n to exit
    requests = requests.encode()

    await requester.communicate(requests)


async def interact_peer(
    label: str, num_peers: int, file_size: str, verbosity: int):
    """
    Creates an extra peer that will connect to the given indexer that can 
    be interfaced with the terminal
    """
    user = 'interact'
    log = LOGS / label / PEERS / f'{user}.log'
    peer_dir = PEERS / file_size / user
    config = CONFIGS / 'eval-peer.ini'

    if peer_dir.exists():
        shutil.rmtree(peer_dir)

    peer_dir.mkdir(parents=True)
    peer = await aio.create_subprocess_exec(
        *shlex.split(
            f'./{PEER} -c {config}  -p {PEER_PORT_BASE + num_peers + 2} '
            f'-d "{peer_dir}" -u "{user}" -l "{log}" -v {verbosity}'))

    await peer.wait()


async def stop_peers(peers: Iterable[PeerRun]):
    """ Sends input to all the clients that should make them cleanly exit """

    async def stop(peer: PeerRun):
        """ Checked stop communication """
        if peer.process is None:
            print('process is None')

        elif peer.process.returncode is None:
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
            await aio.wait_for(peer.stdout.read(CHUNK_SIZE), timeout=6)

    except (aio.TimeoutError, aio.CancelledError):
        pass



#region indexer peers

async def start_indexer(label: str) -> proc.Process:
    """
    Launches the server on a separate process with the specified log and 
    directory
    """
    config = CONFIGS / 'eval-indexer.ini'
    log = LOGS / label / f'indexer.log'

    # clear log content
    if log.exists():
        with open(log, 'w'): pass

    server = await aio.create_subprocess_exec(
        *shlex.split(f'./{INDEXER} -c {config} -l "{log}"'),
        stdin=proc.PIPE,
        stdout=proc.PIPE
    )

    await get_response(server)
    await aio.sleep(2)
    # not fully verification server is set up
    # TODO: better sync primitive for server set up

    return server


async def stop_indexer(server: proc.Process):
    """ Sends input to the server that should make it cleanly exit """
    await server.communicate('\n'.encode())

#endregion


async def run_cycle(
    num_peers: int,
    file_size: str,
    requests: int,
    min_copies: int,
    verbosity: int,
    interactive: bool,
    dry_run: Optional[str]):
    """
    Manages the creation, running, and killing of server and client procs
    """
    if requests <= 0:
        requests = min_copies

    init_server_files(file_size, min_copies)
    label = f'{num_peers}p{file_size}f'

    if dry_run is not None:
        dry_run_peers(
            dry_run, label=label, num_peers=num_peers, file_size=file_size)
        return

    indexer = await start_indexer(label)
    peers = await create_peers(label, num_peers, file_size, verbosity)

    try:
        # interact done first, so that peers don't end early
        if interactive:
            await interact_peer(label, num_peers, file_size, verbosity)
        else:
            await run_downloads(label, num_peers, file_size, requests)

    except Exception as e:
        raise e

    finally:
        await stop_peers(peers)
        await stop_indexer(indexer)



if __name__ == "__main__":
    if sys.version_info < (3,9):
        raise RuntimeError("Python version needs to be at least 3.9")

    args = ArgumentParser(
        description = 'Runs various configurations for peer clients')

    args.add_argument('-d', '--dry-run',
        nargs = '?',
        const = '',
        help='run peer init without starting the processes; '
             'file path can be given to reset dir files')

    args.add_argument('-f', '--file-size',
        choices = ['1m'],
        default = '1m',
        help = 'the size of each file downloaded')

    args.add_argument('-i', '--interactive',
        action = 'store_true',
        help = 'creates an extra interactive peer to interface '
               'with the system')

    args.add_argument('-m', '--min-copies',
        type = int,
        default = 10,
        help = 'the minimum number of files per peer')

    args.add_argument('-n', '--num-peers',
        type = int,
        default = 2, 
        help = 'the number of concurrent clients')

    args.add_argument('-r', '--requests',
        type = int,
        default = -1,
        help = 'total amount of query requests')

    args.add_argument('-v', '--verbosity',
        type = int,
        choices = [0, 10, 20, 30, 40, 50],
        default = 10,
        help = 'the logging verboseness, level corresponds to '
               'default levels')

    args = args.parse_args()
    aio.run(run_cycle(**vars(args)))
