import sys
import argparse
import asyncio as aio
import shlex

PYTHON_CMD = "python" if sys.platform.startswith('win') else "python3"

async def main():
    server = await aio.create_subprocess_exec(
        *shlex.split(f"{PYTHON_CMD} server.py -c configs/default-server.ini"),
        stdin=aio.subprocess.PIPE
    )

    await aio.sleep(2) # await to init a bit
    await server.communicate('\n'.encode())


if __name__ == "__main__":
    if sys.version_info < (3, 8):
        raise RuntimeError("Python version needs to be at least 3.8")
