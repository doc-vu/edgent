import os
log_dir='/home/kharesp/log/colocated_topics_5'
broker_mem_capacity=2
broker_nw_capacity=1
toGb=.000008

runs=[int(config) for config in os.listdir(log_dir) if os.path.isdir('%s/%s'%(log_dir,config)) ]

with open('%s/output.csv'%(log_dir),'w') as f:
  f.write('foreground_#pub,foreground_#sub,foreground_rate,#colocated_topics,background_#pub,background_#sub,background_rate,cpu_utilization(%),iowait(%),memory_utilization(%),network_utilization(%),90th_percentile_latency(ms)\n')
  #f.write('foreground_#pub,foreground_#sub,foreground_rate,#colocated_topics,background_#endpoints,background_rate,cpu_utilization(%),iowait(%),memory_utilization(%),network_utilization(%),90th_percentile_latency(ms)\n')
  for run in sorted(runs):
    with open('%s/%d/features'%(log_dir,run),'r') as inp:
      #skip header 
      next(inp)
      features=next(inp).rstrip()
    with open('%s/%d/summary/summary_util.csv'%(log_dir,run),'r') as inp:
      #skip header
      next(inp)
      node,cpu,iowait,mem,nw=next(inp).split(',')
      percent_mem=(100*float(mem))/16
      percent_nw=100*toGb*float(nw)
    with open('%s/%d/summary/summary_topic.csv'%(log_dir,run),'r') as inp:
      #skip header
      next(inp)
      values=next(inp).split(',')
      latency_90th_percentile=float(values[9])
    f.write('%s,%s,%s,%f,%f,%f\n'%\
      (features,cpu,iowait,percent_mem,percent_nw,latency_90th_percentile))
