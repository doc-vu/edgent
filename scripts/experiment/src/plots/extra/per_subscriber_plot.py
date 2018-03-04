import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import metadata
import numpy as np
import matplotlib.pyplot as plt

def process(log_dir):
  for f in os.listdir(log_dir):
    if (os.path.isfile(os.path.join(log_dir,f)) and f.startswith('t')):
      process_topic_file(log_dir,f)

def process_topic_file(log_dir,f):
  filename,ext=f.split('.')
  topic,hostname,pid=filename.split('_')

  if(not os.path.exists('%s/plots/per_subscriber_plots/%s/latency'%\
    (log_dir,topic))):
    os.makedirs('%s/plots/per_subscriber_plots/%s/latency'%\
      (log_dir,topic))
  if(not os.path.exists('%s/plots/per_subscriber_plots/%s/latency_to_eb'%\
    (log_dir,topic))):
    os.makedirs('%s/plots/per_subscriber_plots/%s/latency_to_eb'%\
      (log_dir,topic))
  if(not os.path.exists('%s/plots/per_subscriber_plots/%s/latency_from_eb'%\
    (log_dir,topic))):
    os.makedirs('%s/plots/per_subscriber_plots/%s/latency_from_eb'%\
      (log_dir,topic))

  data=np.genfromtxt('%s/%s'%(log_dir,f),
    dtype='int,int,int',delimiter=',',\
    usecols=[4,6,7],skip_header=1)[metadata.initial_samples:] 
  xvals=np.arange(len(data))

  #latency(ms) vs sample_id
  plt.plot(xvals,data['f0'])
  plt.xticks(rotation=30)
  plt.xlabel('sample id')
  plt.ylabel('latency (ms)')
  plt.title('%s'%(filename))
  plt.savefig('%s/plots/per_subscriber_plots/%s/latency/%s_latency.png'%\
    (log_dir,topic,filename))
  plt.close()

  #latency_to_eb(ms) vs sample_id
  plt.plot(xvals,data['f1'])
  plt.xticks(rotation=30)
  plt.xlabel('sample id')
  plt.ylabel('latency to eb(ms)')
  plt.title('%s'%(filename))
  plt.savefig('%s/plots/per_subscriber_plots/%s/latency_to_eb/%s_latency_to_eb.png'%\
    (log_dir,topic,filename))
  plt.close()

  #latency_from_eb(ms) vs sample_id
  plt.plot(xvals,data['f2'])
  plt.xticks(rotation=30)
  plt.xlabel('sample id')
  plt.ylabel('latency from eb(ms)')
  plt.title('%s'%(filename))
  plt.savefig('%s/plots/per_subscriber_plots/%s/latency_from_eb/%s_latency_from_eb.png'%\
    (log_dir,topic,filename))
  plt.close()

if __name__=="__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for plotting per subscriber latency plot')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()

  for sub_dir in args.sub_dirs:
    #ensure log_dir/sub_dir/plots and log_dir/sub_dir/summary directories exist
    if not os.path.exists('%s/%s/plots'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/plots'%(args.log_dir,sub_dir))
    #process latency files in log_dir/i
    process('%s/%s'%(args.log_dir,sub_dir))
