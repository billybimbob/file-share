#!/usr/bin/env python3

from argparse import ArgumentParser
from pathlib import Path
from typing import Union
import shutil


def rename_dir(dir: Union[str, Path], subdirs: bool):
    print(f'renaming the files in dir {dir}')

    if isinstance(dir, Path):
        path = dir
    else:
        path = Path(dir)

    base = path.name

    files = [
        p
        for p in path.iterdir()
        if p.is_file()
    ]
    new_files = [
        f.with_name(
            f"{base}({i})" if i > 0 else base
        ).with_suffix(''.join(f.suffixes))
        for i, f in enumerate(files)
    ]

    for old, new in zip(files, new_files):
        shutil.move(str(old), str(new))

    if subdirs:
        for p in path.iterdir():
            if not p.is_dir():
                continue 

            rename_dir(p, subdirs)


if __name__ == "__main__":
    args = ArgumentParser(description="renames all the files in a directory to match the dir name")
    args.add_argument("-d", "--dir", required=True, help="the directory where all the files will be modified")
    args.add_argument("-s", "--subdirs", action='store_true', help="apply to also the subdirectories")
    args = args.parse_args()

    rename_dir(args.dir, args.subdirs)
