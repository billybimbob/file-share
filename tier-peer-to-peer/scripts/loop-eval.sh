#!/usr/bin/env bash

requests=300
num_peers=(1 2 4 8)

for num in ${num_peers[@]}; do
    ./scripts/evaluation.py -n $num -f "128" -r $requests -m topology/ten-all.json
done

for num in ${num_peers[@]}; do
    ./scripts/evaluation.py -n $num -f "128" -r $requests -m topology/ten-line.json
done