#!/bin/bash

for i in `seq 31 40`;
do
  python src/lb/static.py  -topics 2 -zk_connector  129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 -fe_address 10.20.30.1 -log_dir ~/log/test  -run_id $i 
done
