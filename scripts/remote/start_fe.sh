#!/bin/bash

if [ $# -ne 1 ];then
  echo 'usage:' $0 'zkConnector'
  exit 1
fi

zkConnector=$1

mkdir -p ~/infrastructure_log/fe

( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.fe.Frontend $zkConnector  1>~/infrastructure_log/fe/fe.log 2>&1 ) & ) 
sleep 2
