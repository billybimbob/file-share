#!/usr/bin/env bash

requests=10
num_peers=(2 4 8 16)

for num in ${num_peers[@]}; do
    tot_requests=$(($requests * $num))
    ./scripts/evaluation.py -n $num -r $tot_requests -m $requests
done
