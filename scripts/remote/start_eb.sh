#!/bin/bash

mkdir -p ~/infrastructure_log/eb

( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.brokers.EdgeBroker 1>~/infrastructure_log/eb/eb.log 2>&1 ) & ) 
