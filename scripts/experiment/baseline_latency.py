import subprocess

def run_test(num_publishers):
  #test params
  test_file='src/tests/subscriber_stress_test_hw1.py'
  payload=4000
  sleep_interval=100
  sample_count=600
  log_dir='/home/kharesp/log/baseline_latency'
  num_subscribers=1 
 
  #run test 
  subprocess.check_call(['python',test_file,'test',str(num_subscribers),'1',str(num_subscribers),str(num_publishers),str(sleep_interval),str(payload),str(sample_count),log_dir])
  subprocess.check_call(['python','src/plots/summarize/summarize.py','-log_dir',log_dir,'-sub_dirs',str(num_subscribers)])

  #rename 
  if not (num_subscribers==num_publishers):
    command_string='cd %s && mv %d %d'%(log_dir,num_subscribers,num_publishers)
    subprocess.check_call(['bash','-c',command_string])  

if __name__=="__main__":
  for num_pub in range(10,0,-1):
    run_test(num_pub)
