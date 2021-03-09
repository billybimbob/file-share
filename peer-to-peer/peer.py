#!/usr/bin/env python3

from __future__ import annotations
from argparse import ArgumentParser

import logging

from connection import (
    version_check
)


if __name__ == "__main__":
    version_check()
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    args = ArgumentParser("creates a peer node")

    args = args.parse_args()