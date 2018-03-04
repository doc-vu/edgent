import argparse,os,sys
import numpy as np

def process(log_dir,sub_dirs):
  role_util_map= process_util(log_dir,sub_dirs)  
  log_util(log_dir,sub_dirs,role_util_map)

def process_util(log_dir,sub_dirs):
  role_util_map={'eb':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
    'sub':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
    'pub':{'cpu':[],'iowait':[],'mem':[],'nw':[]}}
  for sub_dir in sub_dirs:
    util_file='%s/%s/summary/summary_util_by_role.csv'%(log_dir,sub_dir)
    with open(util_file) as f:
      #skip header  
      next(f)
      for line in f:
        role,cpu,iowait,mem,nw=line.split(',')
        role_util_map[role]['cpu'].append(float(cpu))
        role_util_map[role]['iowait'].append(float(iowait))
        role_util_map[role]['mem'].append(float(mem))
        role_util_map[role]['nw'].append(float(nw))
  return role_util_map

def log_util(log_dir,sub_dirs,role_util_map):
 #log observed utilization values 
  with open('%s/summary_utilization.csv'%(log_dir),'w') as f:
    f.write('xticks:%s\n'%(','.join(sub_dir for sub_dir in sub_dirs)))
    for role,utilization in role_util_map.items():
      for resource in utilization:
        yvals=utilization[resource]
        f.write('%s,%s,%s\n'%(role,resource,','.join(str(val) for val in yvals)))
      f.write('\n')

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for collating results across test iterations')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()
 
  process(args.log_dir,args.sub_dirs)
