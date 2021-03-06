#!/usr/bin/env python3

from __future__ import annotations
from typing import Any, NamedTuple, Optional

from argparse import ArgumentParser
from pathlib import Path

import json
import re
import matplotlib.pyplot as plt


TIMES = Path('times')

def read_response_times(log: Path) -> list[float]:
    """ Parse log files to extract client download times """
    times: list[float] = []
    with open(log) as f:
        for line in f:
            line = line.rstrip()
            if not line.endswith('secs'):
                continue

            toks = line.split()
            if 'downloaded' in toks:
                times.append(float(toks[-2]))

    return times


class Label(NamedTuple):
    """ Runtime classification Label, handles string parsing """
    num_peers: int

    def __str__(self):
        return f'{self.num_peers} Peers'


    @staticmethod
    def from_key(label_info: str) -> Optional[Label]:
        """ Tries to parse a Label from a json key """
        toks = label_info.split()
        try:
            return Label(int(toks[0]))
        except:
            return None

    
    @staticmethod
    def sort(entry: tuple[str, float]) -> int:
        """ Extracts the numerical sortable values from the json key """
        label = Label.from_key(entry[0])
        if label is None:
            return 0
        else:
            return label.num_peers


    @staticmethod
    def get(log: Path) -> Optional[str]:
        """ Parses the log file name to get the configuration settings """
        match = re.search(r'([\d]+)p[^f]+f', log.name)
        if not match:
            return None

        return str(Label(int(match[1])))


def as_time_json(filename: str) -> Path:
    """ Convert file name to be a json file in the times directory """
    return TIMES.joinpath(filename).with_suffix(".json")
    

def record_times(time_file: str, run_label: str, times: list[float]):
    """ Writes or modifies and existing json file with new time data """
    filepath = as_time_json(time_file)
    filepath.parent.mkdir(exist_ok=True, parents=True)
    mode = 'r+' if filepath.exists() else 'w+'

    with open(filepath, mode) as f:
        try:
            obj: dict[str,list[float]] = json.load(f)
        except json.JSONDecodeError:
            obj: dict[str,list[float]] = {}

        if run_label in obj:
            obj[run_label].extend(times)
        else:
            obj[run_label] = times

        # overwrite content
        f.seek(0)
        json.dump(obj, f, indent=4)
        f.truncate()



def record_avgs(time_file: str, avg_file: Optional[str]=None) -> str:
    """
    Truncates the recorded times to an average. One issue with this view 
    is that the quantity of times entries between different run configs
    is lost
    """
    if not avg_file:
        avg_file = time_file

    timepath = as_time_json(time_file)
    timepath.parent.mkdir(exist_ok=True, parents=True)
    avgs: dict[str, Any] = {}
    with open(timepath, 'r') as r:
        file_times: dict[str, list[float]] = json.load(r)
        avgs = {
            label: sum(times) / len(times)
            for label, times in file_times.items()
        }

    avgpath = as_time_json(avg_file)
    with open(avgpath, 'w+') as w:
        json.dump(avgs, w, indent=4)

    return avg_file


def graph_avgs(name: str, avgfile: str, graphpath: Optional[str]):
    """
    Plots values based on the given file. The file is expected to be a
    json object where the is the x-axis, and the value is the y-axis
    """
    avgpath = as_time_json(avgfile)
    with open(avgpath, 'r') as f:
        avgs: dict[str, float] = json.load(f)
        avgs = {
            label: time * 1000 # convert to ms
            for label, time in sorted(
                avgs.items(),
                key=Label.sort
            )
        }

        plt.figure(figsize=(8,6), dpi=80, facecolor='w', edgecolor='k')

        plt.plot(avgs.keys(), avgs.values())
        plt.xlabel("Runtime Configurations")
        plt.ylabel("Time (milliseconds)")
        plt.title(name)

        if graphpath is None:
            plt.show()
        else:
            plt.savefig(f'{TIMES}/{graphpath}')


def parse_and_graph(
    logs: str,
    name: str,
    times: str,
    averages: Optional[str],
    graph: Optional[str]):
    """
    Runs all the steps of parsing, recording, and graphing a given log file
    """
    if (timepath := as_time_json(times)).exists():
        open(timepath, 'w').close()

    logspath = Path(logs)
    read_times(logspath, times)

    averages = record_avgs(times, averages)
    graph_avgs(name, averages, graph)



def read_times(
    logs: Path, time_store: str, top_parent: Optional[Path] = None):
    """ Extract the time values for a file and write them to a json """
    if top_parent is None:
        top_parent = logs

    for log in logs.iterdir():
        if log.is_dir():
            read_times(log, time_store, top_parent)

        elif log.suffix == '.log':
            times = read_response_times(log)
            default_label = log.parent.name
            label = None

            while label is None and log.parent != top_parent:
                label = Label.get(log.parent)
                log = log.parent

            if label is None:
                label = default_label

            record_times(time_store, label, times)


    
if __name__ == "__main__":
    args = ArgumentParser(
        description = 'Parses logging information, and outputs '
                      'to a json and graph')

    args.add_argument('-a', '--averages',
        help = 'the json file where the average times will be '
               'recorded, default will override times arg')

    args.add_argument('-g', '--graph',
        help = 'location where to save the graph')

    args.add_argument('-l', '--logs',
        required = True,
        help = 'the location of the logs to parse and graph')

    args.add_argument('-n', '--name',
        default = 'Average Download Times',
        help = 'the name of the graph')

    args.add_argument('-t', '--times',
        default = 'times.json',
        help='the json file where the times will be recorded')

    args = args.parse_args()
    parse_and_graph(**vars(args))