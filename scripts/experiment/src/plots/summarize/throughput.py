import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np

def process(log_dir):
  topic_files_map={}
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('t')):
      topic=f.partition('_')[0]
      if topic in topic_files_map:
        topic_files_map[topic].append(log_dir+'/'+f)
      else:
        topic_files_map[topic]=[log_dir+'/'+f]
  
  with open('%s/summary/summary_reception_rates.csv'%(log_dir),'w') as f:
    header="""topic,sub_reception_rate(msgs/sec),eb_reception_rate(msgs/sec)\n"""
    f.write(header)

    for topic,files in topic_files_map.items():
      stats=process_topic(log_dir,topic,files)
      f.write('%s,%f,%f\n'%(topic,
        stats['sub_reception_rate'],stats['eb_reception_rate']))
  

def process_topic(log_dir,topic,topic_files):
  stats={}
  stats['sub_reception_rate']= sub_reception_rate(log_dir,topic,topic_files)
  stats['eb_reception_rate']= eb_reception_rate(log_dir,topic,topic_files)
  return stats

def sub_reception_rate(log_dir,topic,topic_files):
  data=[np.genfromtxt(f,dtype='int',delimiter=',',\
    usecols=[0],skip_header=1) for f in topic_files]
  elapsed_time=[(arr[-1]-arr[0]) for arr in data]
  number_of_samples=[(len(arr)*1000.0) for arr in data]
  throughput=np.divide(number_of_samples,elapsed_time)
  return np.mean(throughput)

def eb_reception_rate(log_dir,topic,topic_files):
  data=[np.genfromtxt(f,dtype='int',delimiter=',',\
    usecols=[5],skip_header=1) for f in topic_files]
  elapsed_time=[(arr[-1]-arr[0]) for arr in data]
  number_of_samples=[(len(arr)*1000.0) for arr in data]
  throughput=np.divide(number_of_samples,elapsed_time)
  return np.mean(throughput)

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for processing topic latency files')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()
  
  for sub_dir in args.sub_dirs:
    #ensure log_dir/sub_dir/plots and log_dir/sub_dir/summary directories exist
    if not os.path.exists('%s/%s/plots'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/plots'%(args.log_dir,sub_dir))
    if not os.path.exists('%s/%s/summary'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/summary'%(args.log_dir,sub_dir))
    #process latency files in log_dir/i
    process('%s/%s'%(args.log_dir,sub_dir))
