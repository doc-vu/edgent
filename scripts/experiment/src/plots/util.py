import os,numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
import datetime as dt
import argparse

def process_nw(log_dir):
  #collect nw files
  nw_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('nw')):
      nw_files.append(log_dir+'/'+f)

  #map for host to avg network usage 
  host_nw_map={}
  for f in nw_files:
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[1]
    #extract ts,rxkB/sec and txkB/sec from file
    data=np.genfromtxt(f,dtype='int,float,float',delimiter=';',\
      usecols=[2,6,7],skip_header=1)
    ts=data['f0'] 
    total_nw=data['f1']+data['f2']

    #add average network usage to host mapping
    host_nw_map[hostname]=np.mean(total_nw)

    #plot nw usage vs ts
    ax=plt.gca()
    xfmt=md.DateFormatter('%Y-%m-%d %H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)
    plt.xticks(rotation=25)
    plt.subplots_adjust(bottom=.2)

    #transform ts to datetimes
    datetimes=[dt.datetime.fromtimestamp(t) for t in ts]
    #plot nw usage vs datetime 
    plt.plot(datetimes,total_nw)
    plt.xlabel('timestamp')
    plt.ylabel('total nw usage (kB/sec)')
    plt.savefig('%s/plots/nw_%s.png'%(log_dir,hostname))
    plt.close()

  return host_nw_map

def process_cpu(log_dir):
  #collect util files
  util_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('util')):
      util_files.append(log_dir+'/'+f)
  
  host_cpu_map={}
  for f in util_files:
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[1]
    #extract ts,%cpu usage
    data=np.genfromtxt(f,dtype='int,float',delimiter=';',\
      usecols=[2,4],skip_header=1)
    ts=data['f0'] 
    cpu=data['f1']
    
    #add average %cpu usage to host mapping
    host_cpu_map[hostname]= np.mean(cpu)
    
    #plot %cpu usage vs ts
    ax=plt.gca()
    xfmt=md.DateFormatter('%Y-%m-%d %H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)
    plt.xticks(rotation=25)
    plt.subplots_adjust(bottom=.2)

    #transform ts to datetimes
    datetimes=[dt.datetime.fromtimestamp(t) for t in ts]
    #plot %cpu usage vs datetime 
    plt.plot(datetimes,cpu)
    plt.xlabel('timestamp')
    plt.ylabel('cpu usage (%)')
    plt.savefig('%s/plots/cpu_%s.png'%(log_dir,hostname))
    plt.close()

  return host_cpu_map
      
def process_mem(log_dir):
  #collect util files
  util_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('util')):
      util_files.append(log_dir+'/'+f)
  
  host_mem_map={}
  for f in util_files:
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[1]
    #extract ts,%mem usage
    data=np.genfromtxt(f,dtype='int,float',delimiter=';',\
      usecols=[2,12],skip_header=1)
    ts=data['f0'] 
    mem=data['f1']
    
    #add average mem usage to host mapping
    host_mem_map[hostname]= np.mean(mem)
    
    #plot mem usage vs ts
    ax=plt.gca()
    xfmt=md.DateFormatter('%Y-%m-%d %H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)
    plt.xticks(rotation=25)
    plt.subplots_adjust(bottom=.2)

    #transform ts to datetimes
    datetimes=[dt.datetime.fromtimestamp(t) for t in ts]
    #plot %mem usage vs datetime 
    plt.plot(datetimes,mem)
    plt.xlabel('timestamp')
    plt.ylabel('memory usage (%)')
    plt.savefig('%s/plots/mem_%s.png'%(log_dir,hostname))
    plt.close()

  return host_mem_map

def process(log_dir):
  host_cpu_map=process_cpu(log_dir)
  host_mem_map=process_mem(log_dir)
  host_nw_map=process_nw(log_dir)
  
  host_util_map={ hostname: [value] +\
    [host_mem_map[hostname]] +\
    [host_nw_map[hostname]] for hostname,value in host_cpu_map.items()}

  with open('%s/summary/summary_util.csv'%(log_dir),'w') as f: 
    f.write('hostname,avg_cpu(%),avg_mem(%),avg_nw(kB/sec)\n')
    for hostname in host_cpu_map.keys():
      f.write('%s,%f,%f,%f\n'%(hostname,\
        host_cpu_map[hostname],\
        host_mem_map[hostname],\
        host_nw_map[hostname]))

if __name__=="__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for processing util files')
  parser.add_argument('log_dir',help='path to log directory')
  parser.add_argument('min_count',type=int,help='starting count')
  parser.add_argument('step_size',type=int,help='step size with which to iterate')
  parser.add_argument('max_count',type=int,help='ending count')
  args=parser.parse_args()

  for i in range(args.min_count,args.max_count+args.step_size,args.step_size):
    #ensure log_dir/i/plots and log_dir/i/summary directories exist
    if not os.path.exists('%s/%d/plots'%(args.log_dir,i)):
      os.makedirs('%s/%d/plots'%(args.log_dir,i))
    if not os.path.exists('%s/%d/summary'%(args.log_dir,i)):
      os.makedirs('%s/%d/summary'%(args.log_dir,i))
    #process util files in log_dir/i
    process('%s/%d'%(args.log_dir,i))
