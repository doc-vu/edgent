import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import metadata
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def process(log_dir):
  topic_files_map={}
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('t')):
      topic=f.partition('_')[0]
      if topic in topic_files_map:
        topic_files_map[topic].append(log_dir+'/'+f)
      else:
        topic_files_map[topic]=[log_dir+'/'+f]
  
  with open('%s/summary/summary_topic.csv'%(log_dir),'w') as f:
    header="""topic,#subscribers,\
avg_latency(ms),\
min_latency(ms),\
max_latency(ms),\
50th_percentile_latency(ms),\
60th_percentile_latency(ms),\
70th_percentile_latency(ms),\
80th_percentile_latency(ms),\
90th_percentile_latency(ms),\
99th_percentile_latency(ms),\
99.99th_percentile_latency(ms),\
99.9999th_percentile_latency(ms),\
avg_latency_to_eb(ms),\
avg_latency_from_eb(ms)\n"""
    f.write(header)

    for topic,files in topic_files_map.items():
      stats=process_topic(log_dir,topic,files)
      f.write('%s,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n'%(topic,
        len(files),
        stats['latency_avg'],
        stats['latency_min'],
        stats['latency_max'],
        stats['latency_50th'],
        stats['latency_60th'],
        stats['latency_70th'],
        stats['latency_80th'],
        stats['latency_90th'],
        stats['latency_99th'],
        stats['latency_99_99th'],
        stats['latency_99_9999th'],
        stats['avg_latency_to_eb'],
        stats['avg_latency_from_eb']))

  with open('%s/summary/overall_performance.csv'%(log_dir),'w') as f:
    data=np.genfromtxt('%s/summary/summary_topic.csv'%(log_dir),
      dtype='float,float,float,float,float,float,float,float,float,float,float,float,float',
      delimiter=',',skip_header=1,usecols=[2,3,4,5,6,7,8,9,10,11,12,13,14])
    latency_avg=np.mean(data['f0'])
    latency_min=np.mean(data['f1'])
    latency_max=np.mean(data['f2'])
    latency_50th=np.mean(data['f3'])
    latency_60th=np.mean(data['f4'])
    latency_70th=np.mean(data['f5'])
    latency_80th=np.mean(data['f6'])
    latency_90th=np.mean(data['f7'])
    latency_99th=np.mean(data['f8'])
    latency_99_99th=np.mean(data['f9'])
    latency_99_9999th=np.mean(data['f10'])
    latency_to_eb=np.mean(data['f11'])
    latency_from_eb=np.mean(data['f12'])
    header="""avg_latency(ms),\
min_latency(ms),\
max_latency(ms),\
50th_percentile_latency(ms),\
60th_percentile_latency(ms),\
70th_percentile_latency(ms),\
80th_percentile_latency(ms),\
90th_percentile_latency(ms),\
99th_percentile_latency(ms),\
99.99th_percentile_latency(ms),\
99.9999th_percentile_latency(ms),\
avg_latency_to_eb(ms),\
avg_latency_from_eb(ms)\n"""
    f.write(header)
    f.write('%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n'%(latency_avg,latency_min,latency_max,
      latency_50th,latency_60th,latency_70th,latency_80th,
      latency_90th,latency_99th,latency_99_99th,
      latency_99_9999th,latency_to_eb,latency_from_eb))

def process_topic(log_dir,topic,topic_files):
  latency_avg=[]
  latency_min=[]
  latency_max=[]
  latency_50th=[]
  latency_60th=[]
  latency_70th=[]
  latency_80th=[]
  latency_90th=[]
  latency_99th=[]
  latency_99_99th=[]
  latency_99_9999th=[]
  avg_latency_to_eb=[]
  latency_to_eb_90th=[]
  avg_latency_from_eb=[]
  
  for f in topic_files:
    data=np.genfromtxt(f,dtype='int,int,int',delimiter=',',\
      usecols=[4,6,7],skip_header=1)[metadata.initial_samples:]

    sorted_latency=np.sort(data['f0'])

    latency_avg.append(np.mean(data['f0']))
    latency_min.append(sorted_latency[0])
    latency_max.append(sorted_latency[-1])
    latency_50th.append(np.percentile(sorted_latency,50))
    latency_60th.append(np.percentile(sorted_latency,60))
    latency_70th.append(np.percentile(sorted_latency,70))
    latency_80th.append(np.percentile(sorted_latency,80))
    latency_90th.append(np.percentile(sorted_latency,90))
    latency_99th.append(np.percentile(sorted_latency,99))
    latency_99_99th.append(np.percentile(sorted_latency,99.99))
    latency_99_9999th.append(np.percentile(sorted_latency,99.9999))
    avg_latency_to_eb.append(np.mean(data['f1']))
    latency_to_eb_90th.append(np.percentile(np.sort(data['f1']),90))
    avg_latency_from_eb.append(np.mean(data['f2']))

  print('90th percentile latency to eb:%f'%(np.mean(latency_to_eb_90th)))

  #mean,min,max,50th,60th,70th,80th,90th and 99th percentile latency values
  stats={}
  stats['latency_avg']= np.mean(latency_avg)
  stats['latency_min']= np.mean(latency_min) 
  stats['latency_max']= np.mean(latency_max) 
  stats['latency_50th']= np.mean(latency_50th) 
  stats['latency_60th']= np.mean(latency_60th)
  stats['latency_70th']= np.mean(latency_70th)
  stats['latency_80th']= np.mean(latency_80th)
  stats['latency_90th']= np.mean(latency_90th)
  stats['latency_99th']= np.mean(latency_99th)
  stats['latency_99_99th']= np.mean(latency_99_99th)
  stats['latency_99_9999th']= np.mean(latency_99_9999th)
  stats['avg_latency_to_eb']= np.mean(avg_latency_to_eb)
  stats['avg_latency_from_eb']= np.mean(avg_latency_from_eb)

  return stats
    
    
    

#def process_topic(log_dir,topic,topic_files):
#  #extract latency values 
#  data=[np.genfromtxt(f,dtype='int,int,int',delimiter=',',\
#    usecols=[4,6,7],skip_header=1)[metadata.initial_samples:] for f in topic_files]
#  
#  latency=[val for i in range(len(data)) for val in  data[i]['f0']]
#  latency_to_eb=[val for i in range(len(data)) for val in  data[i]['f1']]
#  latency_from_eb=[val for i in range(len(data)) for val in  data[i]['f2']]
#
#  sorted_latency_vals=np.sort(latency)
#
#  #mean,min,max,50th,60th,70th,80th,90th and 99th percentile latency values
#  stats={}
#  stats['latency_avg']= np.mean(sorted_latency_vals)
#  stats['latency_min']= sorted_latency_vals[0] 
#  stats['latency_max']= sorted_latency_vals[-1] 
#  stats['latency_50th']= np.percentile(sorted_latency_vals,50)
#  stats['latency_60th']= np.percentile(sorted_latency_vals,60)
#  stats['latency_70th']= np.percentile(sorted_latency_vals,70)
#  stats['latency_80th']= np.percentile(sorted_latency_vals,80)
#  stats['latency_90th']= np.percentile(sorted_latency_vals,90)
#  stats['latency_99th']= np.percentile(sorted_latency_vals,99)
#  stats['latency_99_99th']= np.percentile(sorted_latency_vals,99.99)
#  stats['latency_99_9999th']= np.percentile(sorted_latency_vals,99.9999)
#  stats['avg_latency_to_eb']= np.mean(latency_to_eb)
#  stats['avg_latency_from_eb']= np.mean(latency_from_eb)
#
#  return stats

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for processing topic latency files')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()

  for sub_dir in args.sub_dirs:
    ##ensure log_dir/sub_dir/plots and log_dir/sub_dir/summary directories exist
    #if not os.path.exists('%s/%s/plots'%(args.log_dir,sub_dir)):
    #  os.makedirs('%s/%s/plots'%(args.log_dir,sub_dir))
    if not os.path.exists('%s/%s/summary'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/summary'%(args.log_dir,sub_dir))
    #process latency files in log_dir/i
    process('%s/%s'%(args.log_dir,sub_dir))
