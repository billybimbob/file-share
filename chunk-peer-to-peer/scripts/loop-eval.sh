#!/usr/bin/env bash

num_files=10
num_peers=(2 4 8 16)

for num in ${num_peers[@]}; do
    echo "Test for ${num} peers"

    ./scripts/evaluation.py -n $num -m $num_files
done
