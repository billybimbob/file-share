#!/usr/bin/env bash

requests=80
num_peers=(2 4 8)
file_size=("128" "512" "2k" "8k" "32k")

for num in ${num_peers[@]}; do
    per_node=$(($requests / $num))
    ./evaluation.py -n $num -f "128" -r $per_node
done

per_node=$(($requests / 4))
for size in ${file_size[@]}; do
    ./evaluation.py -n 4 -f $size -r $per_node
done