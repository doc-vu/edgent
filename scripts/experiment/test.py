import subprocess

#log_directory='/home/kharesp/log'
#start_id=51
#end_id=100
#
#for num_topics in range(15,1,-1):
#  if num_topics==14:
#    start_id=71
#  else:
#    start_id=51
#  ##make directory 
#  command_string='mkdir -p %s/colocated_with_proc_%d'%(log_directory,num_topics)
#  subprocess.check_call(['bash','-c',command_string])  
#
#  #run tests
#  subprocess.check_call(['python','src/tests/rate_processing_interval_combinations.py',\
#    '-log_dir','%s/colocated_with_proc_%d'%(log_directory,num_topics),'-topics','%d'%(num_topics),
#    '-start_id', '%d'%(start_id),'-end_id','%d'%(end_id)])

log_directory='/home/kharesp/log'
start_id=1
end_id=10

num_topics=1
for run_id in range(1,6,1):
  ##make directory 
  command_string='mkdir -p %s/colocation_impact/isolated/run%d'%(log_directory,run_id)
  subprocess.check_call(['bash','-c',command_string])  
  
  #run tests
  subprocess.check_call(['python','src/tests/rate_processing_interval_combinations.py',\
    '-config_dir','%s/colocation_impact'%(log_directory),\
    '-log_dir','%s/colocation_impact/isolated/run%d'%(log_directory,run_id),\
    '-topics','%d'%(num_topics),\
    '-start_id', '%d'%(start_id),\
    '-end_id','%d'%(end_id)])
