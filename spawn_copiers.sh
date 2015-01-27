#!/bin/bash
for ser in $(python echo_upmus.py)
do
    echo "starting $ser"
    screen -S $ser -d -m python sync2_q.py $ser
done

