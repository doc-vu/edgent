import subprocess

def extract_90th_percentile_latency(run_id,log_dir):
  summary_file='%s/%d/summary/overall_performance.csv'%(log_dir,run_id)
  with open(summary_file,'r') as f:
    #skip header
    f.next()
    return float(f.next().split(',')[3])
   

def run_test(hw_type):
  #test params
  test_file='src/tests/subscriber_stress_test_hw%d.py'%(hw_type)
  payload=4000
  sleep_interval=100
  sample_count=600
  num_publishers=10
  log_dir='/home/kharesp/log/max_sub'
  
  num_subscribers=1 
  while True:
    subprocess.check_call(['python',test_file,'test',str(num_subscribers),'1',str(num_subscribers),str(num_publishers),str(sleep_interval),str(payload),str(sample_count),log_dir])
    subprocess.check_call(['python','src/plots/summarize/summarize.py','-log_dir',log_dir,'-sub_dirs',str(num_subscribers)])
    if extract_90th_percentile_latency(num_subscribers,log_dir)>4:
      break
    num_subscribers+=1

  #make directory 
  command_string='mkdir %s/hw%d'%(log_dir,hw_type)
  subprocess.check_call(['bash','-c',command_string])  

  #move test executions to above created directory
  command_string='cd %s && mv %s hw%d'%(log_dir,' '.join(str(i) for i in range(1,num_subscribers+1)),hw_type)
  subprocess.check_call(['bash','-c',command_string])  

if __name__=="__main__":
  for type in range(1,6):
    run_test(type)
