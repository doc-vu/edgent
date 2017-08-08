import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
90th_percentile_latency(ms),\
99th_percentile_latency(ms),\
99.99th_percentile_latency(ms),\
99.9999th_percentile_latency(ms)\n"""
    f.write(header)

    for topic,files in topic_files_map.items():
      stats=process_topic(log_dir,topic,files)
      f.write('%s,%d,%f,%f,%f,%f,%f,%f,%f\n'%(topic,
        len(files),
        stats['latency_avg'],
        stats['latency_min'],
        stats['latency_max'],
        stats['latency_90th'],
        stats['latency_99th'],
        stats['latency_99_99th'],
        stats['latency_99_9999th']))
  

def process_topic(log_dir,topic,topic_files):
  #extract latency values 
  data=[np.genfromtxt(f,dtype='int',delimiter=',',\
    usecols=[3],skip_header=1)[metadata.initial_samples:] for f in topic_files]

   
  sorted_latency_vals=np.sort(np.concatenate(data))

  #plot cdf
  yvals=np.arange(1,len(sorted_latency_vals)+1)/float(len(sorted_latency_vals))
  plt.plot(sorted_latency_vals,yvals)
  plt.xlabel('latency(ms)')
  plt.ylabel('cdf')
  plt.savefig('%s/plots/cdf_%s.png'%(log_dir,topic))
  plt.close()

  #mean,min,max,90th and 99th percentile latency values
  stats={}
  stats['latency_avg']= np.mean(sorted_latency_vals)
  stats['latency_min']= np.min(sorted_latency_vals)
  stats['latency_max']= np.max(sorted_latency_vals)
  stats['latency_90th']= np.percentile(sorted_latency_vals,90)
  stats['latency_99th']= np.percentile(sorted_latency_vals,99)
  stats['latency_99_99th']= np.percentile(sorted_latency_vals,99.99)
  stats['latency_99_9999th']= np.percentile(sorted_latency_vals,99.9999)

  return stats

        

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for processing topic latency files')
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
    #process latency files in log_dir/i
    process('%s/%d'%(args.log_dir,i))
