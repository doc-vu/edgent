import os,sys,numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import conf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
import datetime as dt
import argparse
import pandas as pd

#Returns a map from hostname to cpu utilization metrics: { hostname: { cpu:%, iowait:%}}
def process_cpu(log_dir):
  #collect util files
  util_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('cpu')):
      util_files.append(log_dir+'/'+f)
  
  host_cpu_map={}
  for f in util_files:
    print(f)
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[1]
    data=pd.read_csv(f,delimiter=';',names=None,header=None,skiprows=1)
    #strip off first three colums: hostname,interval,ts
    data=data.iloc[:,3:]
    #core_id,%user,%nice,%system,%iowait,%steal,%idle
    num_cores=data.shape[1]/7
    num_readings=data.shape[0]

    cpu_user=0
    cpu_system=0
    cpu_iowait=0
    for core in range(int(num_cores)):
      cpu_user+= np.sum(data.iloc[:,7*core+1])
      cpu_system+= np.sum(data.iloc[:,7*core+3])
      cpu_iowait+= np.sum(data.iloc[:,7*core+4])
   
    avg_cpu= (cpu_user+cpu_system)/(num_cores*num_readings)  
    avg_iowait= cpu_iowait/(num_cores*num_readings)
    ##add average %cpu usage to host mapping
    host_cpu_map[hostname]= {'cpu':avg_cpu,'iowait':avg_iowait} 
  return host_cpu_map
      
def process_mem(log_dir):
  #collect util files
  util_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('mem')):
      util_files.append(log_dir+'/'+f)
  
  host_mem_map={}
  for f in util_files:
    print(f)
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[1]
    #extract kbmemused 
    data=np.genfromtxt(f,dtype='int',delimiter=';',\
      usecols=[4],skip_header=1)
    mem_gb=data/1000000.0
    
    #add average mem usage to host mapping
    host_mem_map[hostname]= np.mean(mem_gb)
  return host_mem_map

def process_nw(log_dir):
  #collect nw files
  nw_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('nw_filtered')):
      nw_files.append(log_dir+'/'+f)

  #map for host to avg network usage 
  host_nw_map={}
  for f in nw_files:
    print(f)
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[2]
    #extract ts,rxkB/sec and txkB/sec from file
    data=np.genfromtxt(f,dtype='int,float,float',delimiter=',',\
      usecols=[0,2,3],skip_header=1)
    ts=data['f0'] 
    total_nw=data['f1']+data['f2']

    #add average network usage to host mapping
    host_nw_map[hostname]=np.mean(total_nw)
  return host_nw_map

def process(log_dir):
  host_cpu_map=process_cpu(log_dir)
  host_mem_map=process_mem(log_dir)
  host_nw_map=process_nw(log_dir)

  with open('%s/summary/summary_util.csv'%(log_dir),'w') as f: 
    f.write('hostname,avg_cpu(%),avg_iowait(%),avg_mem(gb),avg_nw(kB/sec)\n')
    for hostname in host_cpu_map.keys():
      f.write('%s,%f,%f,%f,%f\n'%(hostname,\
        host_cpu_map[hostname]['cpu'],\
        host_cpu_map[hostname]['iowait'],\
        host_mem_map[hostname],\
        host_nw_map[hostname]))

  #with open('%s/summary/summary_util.csv'%(log_dir),'r') as inp,\
  #  open('%s/summary/summary_util_by_role.csv'%(log_dir),'w') as out: 
  #  test_config=conf.Conf('%s/conf'%(log_dir))
  #  ebs=test_config.ebs
  #  subscriber_client_machines=test_config.subscriber_client_machines
  #  publisher_client_machines=test_config.publisher_client_machines
  #  util={'eb':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
  #    'sub':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
  #    'pub':{'cpu':[],'iowait':[],'mem':[],'nw':[]}}
  #  #skip header  
  #  next(inp)
  #  out.write('role,cpu,iowait,mem,nw\n')
  #  for line in inp:
  #    hostname,cpu,iowait,mem,nw= line.split(',')
  #    if(hostname in ebs):
  #      util['eb']['cpu'].append(float(cpu))
  #      util['eb']['iowait'].append(float(iowait))
  #      util['eb']['mem'].append(float(mem))
  #      util['eb']['nw'].append(float(nw))
  #    elif(hostname in subscriber_client_machines):
  #      util['sub']['cpu'].append(float(cpu))
  #      util['sub']['iowait'].append(float(iowait))
  #      util['sub']['mem'].append(float(mem))
  #      util['sub']['nw'].append(float(nw))
  #    elif(hostname in publisher_client_machines):
  #      util['pub']['cpu'].append(float(cpu))
  #      util['pub']['iowait'].append(float(iowait))
  #      util['pub']['mem'].append(float(mem))
  #      util['pub']['nw'].append(float(nw))
  #    else:
  #      print('hostname:%s role not found'%(hostname))
  #  

  #  for role,utilization in util.items():
  #    out.write('%s'%(role))
  #    for resource in ['cpu','iowait','mem','nw']:
  #      out.write(',%f'%(np.mean(utilization[resource])))
  #    out.write('\n')

if __name__=="__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for processing util files')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()

  
  for sub_dir in args.sub_dirs:
    #ensure log_dir/sub_dir/summary directories exist
    if not os.path.exists('%s/%s/summary'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/summary'%(args.log_dir,sub_dir))

    #process util files in log_dir/i
    process('%s/%s'%(args.log_dir,sub_dir))
