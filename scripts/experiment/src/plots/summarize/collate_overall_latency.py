import argparse
def process(log_dir,sub_dirs):
  res={'avg':[],\
    '50th':[],\
    '60th':[],\
    '70th':[],\
    '80th':[],\
    '90th':[],\
    '99th':[],\
    'avg_latency_to_eb':[],\
    'avg_latency_from_eb':[]}
  for sub_dir in sub_dirs:
    latency_file='%s/%s/summary/overall_performance.csv'%(log_dir,sub_dir)
    with open(latency_file) as f:
      #skip header
      next(f)
      line=next(f)
      avg_latency,min_latency,max_latency,\
      latency_50_percentile,latency_60_percentile,\
      latency_70_percentile,latency_80_percentile,\
      latency_90_percentile,latency_99_percentile,\
      latency_99_99_percentile,latency_99_9999_percentile,\
      avg_latency_to_eb,avg_latency_from_eb= line.split(',')
      res['avg'].append(float(avg_latency))
      res['50th'].append(float(latency_50_percentile))
      res['60th'].append(float(latency_60_percentile))
      res['70th'].append(float(latency_70_percentile))
      res['80th'].append(float(latency_80_percentile))
      res['90th'].append(float(latency_90_percentile))
      res['99th'].append(float(latency_99_percentile))
      res['avg_latency_to_eb'].append(float(avg_latency_to_eb))
      res['avg_latency_from_eb'].append(float(avg_latency_from_eb))

  measurements=['avg','50th','60th','70th','80th','90th','99th','avg_latency_to_eb','avg_latency_from_eb']
  with open('%s/overall_performance.csv'%(log_dir),'w') as f:
    f.write('xticks:%s\n'%(','.join(sub_dir for sub_dir in sub_dirs)))
    for measurement in measurements:
      f.write('%s,%s\n'%(measurement,','.join(str(val) for val in res[measurement])))

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for collating results across test iterations')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()
 
  process(args.log_dir,args.sub_dirs)
