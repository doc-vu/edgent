#!/bin/bash

if [ $# -ne 4 ]; then
  echo 'usage:' $0 'outDir runId zkConnector pub_node' 
  exit 1
fi

outDir=$1
runId=$2
zkConnector=$3
pub_node=$4

mkdir -p ~/infrastructure_log/monitor

( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.monitoring.Monitor $outDir $runId $zkConnector $pub_node 1>~/infrastructure_log/monitor/monitoring.log 2>&1 ) & ) 
sleep 2
