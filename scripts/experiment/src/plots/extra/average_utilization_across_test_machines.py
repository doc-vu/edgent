import numpy as np
import argparse

def average_across_host_machines(log_dir,iteration,host_start_range):
  cpu_vals=[]
  iowait_vals=[]
  mem_vals=[]
  nw_vals=[]
  with open('%s/%s/summary/summary_util.csv'%(log_dir,iteration),'r') as f,\
    open('%s/%s/summary/avg_across_hosts.csv'%(log_dir,iteration),'w') as w:
    next(f)
    for line in f:
      host,cpu,iowait,mem,nw= line.split(',')
      id=host[4:]
      if(int(id) >= host_start_range):
        cpu_vals.append(float(cpu))
        iowait_vals.append(float(iowait))
        mem_vals.append(float(mem))
        nw_vals.append(float(nw))

    avg_cpu=np.mean(cpu_vals)
    avg_iowait=np.mean(iowait_vals)
    avg_mem=np.mean(mem_vals)
    avg_nw=np.mean(nw_vals)
    w.write('cpu,iowait,mem,nw\n')
    w.write('%f,%f,%f,%f\n'%(avg_cpu,avg_iowait,avg_mem,avg_nw))
    return {'cpu':avg_cpu,'iowait':avg_iowait,'mem':avg_mem,'nw':avg_nw}

def summarize(log_dir,start_range,step_size,end_range,host_start_range):
  util={'cpu':[],'iowait':[],'mem':[],'nw':[]}
  for iteration in range(start_range,end_range+step_size,step_size):
    util_map= average_across_host_machines(log_dir,iteration,host_start_range)
    util['cpu'].append(util_map['cpu'])
    util['iowait'].append(util_map['iowait'])
    util['mem'].append(util_map['mem'])
    util['nw'].append(util_map['nw'])
  with open('%s/avg_across_nodes.csv'%(log_dir),'w') as f: 
    for resource, values in util.iteritems():
      f.write('%s,%s,\n'%(resource,','.join([str(val) for val in values])))

if __name__=="__main__":
 #parse cmd line args
  parser=argparse.ArgumentParser(description='script for averaging utilization across test machines')
  parser.add_argument('log_dir',help='path to log directory')
  parser.add_argument('min_count',type=int,help='starting count')
  parser.add_argument('step_size',type=int,help='step size with which to iterate')
  parser.add_argument('max_count',type=int,help='ending count')
  parser.add_argument('host_start_range',type=int,help='starting range for physical nodes')
  args=parser.parse_args()
  
  summarize(args.log_dir,args.min_count,args.step_size,args.max_count,args.host_start_range)
