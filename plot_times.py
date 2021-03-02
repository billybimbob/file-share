from typing import Dict, List, Optional
from argparse import ArgumentParser
from pathlib import Path

import json
import re
import matplotlib.pyplot as plt

TIMES = 'times'

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


def get_label(log: str) -> Optional[str]:
    """ Parses the log file name to get the configuration settings """
    match = re.search('[^\\-]+\\-([0-9]+)c([0-9]+)f.log', log)
    if not match:
        return None

    return f'{match[0]} clients, {match[1]}b files'

    

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



def record_avgs(time_file: str, avg_file: Optional[str]=None) -> str:
    """
    Truncates the recorded times to an average. One issue with this view is that the 
    quantity of times entries between different run configs is lost
    """
    if not avg_file:
        avg_file = time_file

    with open(f'{TIMES}/{time_file}', 'r') as r:
        file_times: Dict[str, List[float]] = json.load(r)
        avgs = {
            label: sum(times) / len(times)
            for label, times in file_times.items()
        }

    avgs = Path(f'{TIMES}/{avg_file}').with_suffix('.json')
    with open(avgs, 'w') as w:
        json.dump(avgs, w)

    return avg_file


def graph_avgs(avg_file: str, graph_path: str):
    """
    Plots values based on the given file. The file is expected to be a
    json object where the is the x-axis, and the value is the y-axis
    """
    pass


def parse_and_graph(log: str, times: str, averages: Optional[str], graph: str):
    """ Runs all the steps of parsing, recording, and graphing a given log file """
    down_times = read_download_times(log)
    label = get_label(log)
    if label is None:
        label = log

    record_times(times, label, down_times)
    averages = record_avgs(times, averages)
    graph_avgs(averages, graph)


    
if __name__ == "__main__":
    args = ArgumentParser("Parses logging information, and outputs to a json and graph")
    args.add_argument("-a", "--averages", help="the json file where the average times will be recorded")
    args.add_argument("-g", "--graph", default='', help="location where to save the graph")
    args.add_argument("-l", "--log", required=True, help="the log file to parse and graph")
    args.add_argument("-t", "--times", default="times.json", help="the json file where the times will be recorded")
    args = args.parse_args()

    parse_and_graph(**vars(args))