import os
import numpy as np

log_dir='/home/kharesp/log'
test_runs=range(2,11)

topic_latency={}
for run in test_runs:
  with open('%s/%d/summary/summary_topic.csv'%(log_dir,run),'r') as f:
    next(f)
    for line in f:
      parts=line.split(',')
      if parts[0] in topic_latency:
        topic_latency[parts[0]].append(float(parts[9]))
      else:
        topic_latency[parts[0]]=[float(parts[9])]

for topic in range(8):
  vals=topic_latency['t%d'%(topic+1)]
  print('mean:%f var:%f\n'%(np.mean(vals),np.std(vals)))
