import argparse,os,sys
import topic,util,throughput,rename,filter_nw_interface,filter_initial_sample
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import metadata

def summarize(log_dir,sub_dirs):
  for sub_dir in sub_dirs:
    #ensure log_dir/sub_dir/summary directories exist
    if not os.path.exists('%s/%s/summary'%(log_dir,sub_dir)):
      os.makedirs('%s/%s/summary'%(log_dir,sub_dir))

    #process files in log_dir/i
    print('processing:%s/%s'%(log_dir,sub_dir))
    rename.process('%s/%s'%(log_dir,sub_dir))
    filter_nw_interface.filter(log_dir,sub_dir)
    filter_initial_sample.filter('%s/%s'%(log_dir,sub_dir),\
      metadata.initial_samples_per_pub)
    topic.process('%s/%s'%(log_dir,sub_dir))
    util.process('%s/%s'%(log_dir,sub_dir))
    throughput.process('%s/%s'%(log_dir,sub_dir))
    

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for summarizing test results')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()
  summarize(args.log_dir,args.sub_dirs)
