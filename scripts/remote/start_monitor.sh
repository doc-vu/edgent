#!/bin/bash

if [ $# -ne 3 ]; then
  echo 'usage:' $0 'outDir runId zkConnector' 
  exit 1
fi

outDir=$1
runId=$2
zkConnector=$3

mkdir -p ~/infrastructure_log/monitor

( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.monitoring.Monitor $outDir $runId $zkConnector 1>~/infrastructure_log/monitor/monitoring.log 2>&1 ) & ) 
sleep 2
