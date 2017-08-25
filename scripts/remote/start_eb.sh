#!/bin/bash

if [ $# -ne 2 ];then
  echo 'usage:' $0 'zkConnector ioThreads'
  exit 1
fi

zkConnector=$1
ioThreads=$2

mkdir -p ~/infrastructure_log/eb

( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.brokers.EdgeBroker $zkConnector $ioThreads 1>~/infrastructure_log/eb/eb.log 2>&1 ) & ) 
sleep 2
