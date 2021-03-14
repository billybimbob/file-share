#!/usr/bin/env bash

requests=10
num_peers=(2 4 8)
file_size=("128" "512" "2k" "8k" "32k")

for num in ${num_peers[@]}; do
    ./evaluation.py -n $num -r $requests > out.txt
done

for size in ${file_size[@]}; do
    ./evaluation.py -f $size -r $requests > out.txt
done