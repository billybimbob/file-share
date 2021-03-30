#!/usr/bin/env bash

requests=300
num_peers=(1 2 4 8)

for num in ${num_peers[@]}; do
    ./scripts/evaluation.py -n 20 -f "128" -r $requests -q $num -m topology/ten-all.json
done

for num in ${num_peers[@]}; do
    ./scripts/evaluation.py -n 20 -f "128" -r $requests -q $num -m topology/ten-line.json
done