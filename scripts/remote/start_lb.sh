#!/bin/bash

if [ $# -ne 1 ];then
  echo 'usage:' $0 'zkConnector'
  exit 1
fi

zkConnector=$1

mkdir -p ~/infrastructure_log/lb

( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.loadbalancer.LoadBalancer $zkConnector 1>~/infrastructure_log/lb/lb.log 2>&1 ) & ) 
sleep 2
