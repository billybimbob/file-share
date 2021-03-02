from os import times
from typing import Dict, List, Optional, Tuple
from argparse import ArgumentParser
from pathlib import Path

import json
import re
import matplotlib.pyplot as plt


TIMES = 'times'

def read_download_times(log: Path) -> List[float]:
    """ Parse log files to extract client download times """
    times: List[float] = []
    with open(log) as f:
        for line in f:
            if not line.rstrip().endswith('secs'):
                continue

            toks = line.split()
            times.append(float(toks[-2]))

    return times


def get_label(log: str) -> Optional[str]:
    """ Parses the log file name to get the configuration settings """
    match = re.search(r'[^\-]+\-([0-9]+)c([0-9]+)f.log', log)
    if not match:
        return None

    return f'{match[1]} clients,{match[2]}b files'


def as_time_json(filename: str) -> Path:
    return Path(TIMES).joinpath(filename).with_suffix(".json")
    

def record_times(time_file: str, run_label: str, times: List[float]):
    """ Writes or modifies and existing json file with new time data """
    filepath = as_time_json(time_file)
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



def record_avgs(time_file: str, avg_file: Optional[str]=None) -> str:
    """
    Truncates the recorded times to an average. One issue with this view is that the 
    quantity of times entries between different run configs is lost
    """
    if not avg_file:
        avg_file = time_file

    timepath = as_time_json(time_file)
    avgs = {}
    with open(timepath, 'r') as r:
        file_times: Dict[str, List[float]] = json.load(r)
        avgs = {
            label: sum(times) / len(times)
            for label, times in file_times.items()
        }

    avgpath = as_time_json(avg_file)
    with open(avgpath, 'w+') as w:
        json.dump(avgs, w)

    return avg_file


def get_num_clients(label_pair: Tuple[str, float]) -> int:
    toks = label_pair[0].split()
    if len(toks) > 1 and toks[0].isnumeric:
        return int(toks[0])

    return 0


def graph_avgs(name: str, avgfile: str, graphpath: Optional[str]):
    """
    Plots values based on the given file. The file is expected to be a
    json object where the is the x-axis, and the value is the y-axis
    """
    avgpath = as_time_json(avgfile)
    with open(avgpath, 'r') as f:
        avgs: Dict[str, float] = json.load(f)
        avgs = {
            label: time
            for label, time in sorted(
                avgs.items(),
                key=get_num_clients
            )
        }

        plt.plot(avgs.keys(), avgs.values())
        plt.xlabel("Runtime Configurations")
        plt.ylabel("Time (seconds)")
        plt.title(name)

        if graphpath is None:
            plt.show()
        else:
            plt.savefig(f'{TIMES}/{graphpath}')


def parse_and_graph(logs: str, name: str, times: str, averages: Optional[str], graph: Optional[str]):
    """ Runs all the steps of parsing, recording, and graphing a given log file """
    if (timepath := as_time_json(times)).exists():
        open(timepath, 'w').close()

    logspath = Path(logs)
    for log in logspath.iterdir():
        if log.suffix != '.log':
            continue

        down_times = read_download_times(log)
        label = get_label(log.name)
        if label is None:
            label = log.name

        record_times(times, label, down_times)

    averages = record_avgs(times, averages)
    graph_avgs(name, averages, graph)


    
if __name__ == "__main__":
    args = ArgumentParser("Parses logging information, and outputs to a json and graph")
    args.add_argument("-a", "--averages", help="the json file where the average times will be recorded, default will override times arg")
    args.add_argument("-g", "--graph", help="location where to save the graph")
    args.add_argument("-l", "--logs", required=True, help="the location of the logs to parse and graph")
    args.add_argument("-n", "--name", default='Average Runtimes', help="the name of the graph")
    args.add_argument("-t", "--times", default="times.json", help="the json file where the times will be recorded")
    args = args.parse_args()

    parse_and_graph(**vars(args))