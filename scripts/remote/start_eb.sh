#!/bin/bash

mkdir -p ~/log/eb

( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.brokers.EdgeBroker 1>~/log/eb/eb.log 2>&1 ) & ) 
