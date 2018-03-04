import argparse,os,sys
import topic,util,throughput,rename,filter_nw_interface

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for summarizing test results')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()
  

  for sub_dir in args.sub_dirs:
    #ensure log_dir/sub_dir/plots and log_dir/sub_dir/summary directories exist
    if not os.path.exists('%s/%s/plots'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/plots'%(args.log_dir,sub_dir))
    if not os.path.exists('%s/%s/summary'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/summary'%(args.log_dir,sub_dir))

    #process files in log_dir/i
    print('processing:%s/%s'%(args.log_dir,sub_dir))
    rename.process('%s/%s'%(args.log_dir,sub_dir))
    filter_nw_interface.filter(args.log_dir,sub_dir)
    topic.process('%s/%s'%(args.log_dir,sub_dir))
    util.process('%s/%s'%(args.log_dir,sub_dir))
    throughput.process('%s/%s'%(args.log_dir,sub_dir))
