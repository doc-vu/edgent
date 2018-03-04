import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import metadata
import numpy as np
import matplotlib.pyplot as plt

vals=[]
def process(log_dir):
  for f in os.listdir(log_dir):
    if (os.path.isfile(os.path.join(log_dir,f)) and f.startswith('t')):
      process_topic_file(log_dir,f)
  print(vals)

def process_topic_file(log_dir,f):
  filename,ext=f.split('.')
  topic,hostname,pid=filename.split('_')

  data=np.genfromtxt('%s/%s'%(log_dir,f),
    dtype='int,int,int',delimiter=',',\
    usecols=[4,6,7],skip_header=1)[metadata.initial_samples:] 

  latency=data['f0']
  sorted_latency_vals=np.sort(latency)
  val= np.percentile(sorted_latency_vals,90)
  #vals.append('%.2f'%(val))
  vals.append(val)


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
