#!/bin/bash

# This script uses the ps command to obtain PID and memory usage information
# for a given process name (there can be more than 1 process). The information
# is obtained at regular intervals and saved to a text file. 
#
if [ $# -eq  3 ]; then
    grepArg=$1
    sleepTime=$2
    logDir=$3
else
    echo "Usage: memdrone.sh processName sleepTime logDir";
    exit 1;
fi

hostname=$(hostname -s)

while true; do
    date=$(date)
    psOutput=$(ps -eo pid,rss,comm | grep $grepArg | grep -v grep)
    IFS="
"
    for psItem in $psOutput; do
        echo "$date $psItem " >> "${logDir}/memdrone-${hostname}.txt"
    done

    sleep $sleepTime
done