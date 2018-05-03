import os,argparse,subprocess

def load_configuration(config_path):
  config=[]
  with open(config_path,'r') as f:
    #skip header
    next(f)
    for line in f:
      config.append(line.rstrip())
  return config

def model_features(config):
  res={}
  for tdesc in config:
    parts= tdesc.split(',')
    topic_name=parts[0]
    processing_interval=parts[1]
    publication_rate=parts[2]
    background_rate_x_processing_interval=0
    background_sum_rate=0
    background_sum_processing=0

    for tdesc in config:
      other_topic_name=tdesc.split(',')[0]
      if not other_topic_name==topic_name:
        background_rate_x_processing_interval+= float(tdesc.split(',')[-1])
        background_sum_processing+= int(tdesc.split(',')[1])
        background_sum_rate+= int(tdesc.split(',')[2])
        
    res[topic_name]='%s,%s,%f,%d,%d'%\
      (processing_interval,publication_rate,\
      background_rate_x_processing_interval,\
      background_sum_processing,\
      background_sum_rate)
  return res
  
def model_output(log_dir,run_id):
  toGb=.000008
  res={}
  with open('%s/%s/summary/summary_util.csv'%(log_dir,run_id),'r') as f:
    #skip header:hostname,avg_cpu(%),avg_iowait(%),avg_mem(gb),avg_nw(kB/sec)
    next(f)
    hostname,cpu,iowait,mem,nw= next(f).split(',')
    res['cpu']=float(cpu)
    res['mem']=float(mem)
    res['nw']=float(nw)*toGb

  res['90th']={}
  res['avg']={}
  res['std']={}
  with open('%s/%s/summary/summary_topic.csv'%(log_dir,run_id),'r') as f:
    #skip header:
    next(f)
    for line in f:
      print(line)
      parts=line.split(',')
      res['90th'][parts[0]]=float(parts[9])
      res['avg'][parts[0]]=float(parts[2])
      res['std'][parts[0]]=float(parts[15])
  return res

def collect(log_dir):
  sub_dirs=os.listdir(log_dir)
  sorted_dirs=sorted([int(d) for d in sub_dirs])
  for iteration in sorted_dirs:
    print("\n\nSummarizing results")
    subprocess.check_call(['python','src/plots/summarize/summarize.py',\
      '-log_dir',log_dir,'-sub_dirs',str(iteration)])

    #load configuration
    config=load_configuration('%s/%d/config'%(log_dir,iteration))
    count=len(config)
    features=model_features(config)
    y=model_output(log_dir,iteration)

    #copy model features 
    with open('%s/%d/features'%(log_dir,iteration),'w') as f:
      f.write('test_id,#topics,foreground_processing_interval,foreground_rate,\
background_rate_x_processing_interval,background_sum_processing,background_sum_rate,\
foreground_avg_latency,foreground_latency_std,foreground_90th_percentile_latency,\
broker_cpu,broker_mem,broker_nw\n')
      for i in range(count):
        topic='t%d'%(i+1) 
        x=features[topic] 
        f.write('%d,%d,%s,%f,%f,%f,%f,%f,%f\n'%(iteration,count,x,\
          y['avg'][topic],y['std'][topic],y['90th'][topic],y['cpu'],y['mem'],y['nw']))
 

if __name__ == "__main__":
  parser= argparse.ArgumentParser(description='script for collecting model features and output')
  parser.add_argument('-log_dir',required=True)
  args=parser.parse_args()
  collect(args.log_dir)
