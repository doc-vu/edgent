import os,subprocess

log_dir='/home/kharesp/log'
payload_sizes=[125*2**i for i in range(10)]
sleep_intervals=[5,10,20,100]
data_rates=[200,100,50,10]
pub_sample_count=10000

def create_directory(path):
  if not os.path.exists(path):
    os.makedirs(path)

for i in range(len(data_rates)):
  #create directory for rate
  rate_dir_path='%s/rate_%d'%(log_dir,data_rates[i])
  create_directory(rate_dir_path)
  for payload in payload_sizes:
    print('Test for data rate:%d and payload:%d'%(data_rates[i],payload))
    subprocess.check_call(['python','src/tests/publisher_stress_test.py','--setup','1','1','1','1',str(sleep_intervals[i]),str(payload),str(pub_sample_count)])

    subprocess.check_call(['python','src/plots/summarize/summarize.py','-log_dir',log_dir,'-sub_dirs','1'])
    command_string='mv /home/kharesp/log/1 %s/%d'%(rate_dir_path,payload)
    subprocess.check_call(['bash','-c',command_string])
