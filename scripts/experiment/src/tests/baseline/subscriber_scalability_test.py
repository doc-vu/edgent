import os,subprocess

log_dir='/home/kharesp/log'
payload_sizes=[4000]
#sleep_intervals=[5,10,20,40,80]
#data_rates=[200,100,50,25,12]
sleep_intervals=[80]
data_rates=[12]
pub_sample_count=10000
subscriber_step_size=4
number_of_steps=8

def create_directory(path):
  if not os.path.exists(path):
    os.makedirs(path)

for i in range(len(data_rates)):
  #create directory for rate
  rate_dir_path='%s/rate_%d'%(log_dir,data_rates[i])
  create_directory(rate_dir_path)

  for idx,payload in enumerate(payload_sizes):
    create_directory('%s/%d'%(rate_dir_path,payload))
     
    print('python src/tests/subscriber_stress_test.py --setup %d %d %d\
      1 %s %s %s'%(subscriber_step_size,subscriber_step_size,\
      number_of_steps*subscriber_step_size,\
      str(sleep_intervals[i]),str(payload),str(pub_sample_count)))

    subprocess.check_call(['python','src/tests/subscriber_stress_test.py','--setup',
      '%d'%(subscriber_step_size),
      '%d'%(subscriber_step_size),
      '%d'%(number_of_steps*subscriber_step_size),
      '1',
      str(sleep_intervals[i]),
      str(payload),
      str(pub_sample_count)])

    sub_dirs=range(subscriber_step_size,
      subscriber_step_size*(number_of_steps+1),
      subscriber_step_size)

    command_string=['python','src/plots/summarize/summarize.py',
      '-log_dir',log_dir,'-sub_dirs']
    for sub_dir in sub_dirs:
      command_string.append(str(sub_dir))
    subprocess.check_call(command_string)

    command_string=['python','src/plots/summarize/collate.py',
      '-xlabel','subscribers','-log_dir',log_dir,'-sub_dirs']
    for sub_dir in sub_dirs:
      command_string.append(str(sub_dir))
    subprocess.check_call(command_string)

    command_string='mv /home/kharesp/log/*.csv  %s/%d'%(rate_dir_path,payload)
    subprocess.check_call(['bash','-c',command_string])

    command_string='mv /home/kharesp/log/*.pdf %s/%d'%(rate_dir_path,payload)
    subprocess.check_call(['bash','-c',command_string])

    for sub_dir in sub_dirs:
      command_string='mv /home/kharesp/log/%d %s/%d'%(sub_dir,rate_dir_path,payload)
      subprocess.check_call(['bash','-c',command_string])
