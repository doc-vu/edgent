import os
import numpy as np
log_dir='/home/kharesp/log/wo_curl_pub'
sub_dirs=[1,10,20,40,80,160]

cpu=[]
#gps_update=[]
traffic_update=[]

for sub_dir in sub_dirs:
  with open('%s/%d/summary/summary_topic.csv'%(log_dir,sub_dir),'r') as f:
    #skip header
    next(f)
    t=[] 
    #b=[]
    for line in f:
      #if line.startswith('b'):
      #  b.append(float(line.split(',')[9]))
      if line.startswith('t'):
        t.append(float(line.split(',')[9]))

    #gps_update.append(np.mean(b)/1000)
    traffic_update.append(np.mean(t))
  with open('%s/%d/summary/summary_util.csv'%(log_dir,sub_dir),'r') as f:
    #skip header
    next(f)
    cpu.append(float(f.next().split(',')[1]))
    
#print(gps_update)
#print('Traffic update')
print(traffic_update)
print(cpu)
