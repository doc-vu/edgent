import subprocess,time,os

subscriber_test_machines=['node%d'%(i) for i in range(4,42,1)]

hw1=['node1','node2','node3','node4','node5','node6','node7','node8','node9','node10',\
  'node11','node12','node13','node14','node15','node16']
hw2=['node27','node28','node29','node30','node31','node32','node33','node34','node35',\
  'node36','node37','node38','node39','node40','node41']
hw3=['node18','node19','node20','node21','node22','node23','node24']
hw4=['node17','node26']
hw5=['node25']

def get_hw_type(node):
  if node in hw1:
    return 'hw1'
  elif node in hw2:
    return 'hw2'
  elif node in hw3:
    return 'hw3'
  elif node in hw4:
    return 'hw4'
  elif node in hw5:
    return 'hw5'
  else:
    return 'invalid'

sub_per_hw_type={'r100':{'hw1':10,'hw2':20,'hw3':35,'hw4':45,'hw5':45}}

max_sub={'p1':800,
  'p2':800,
  'p3':800,
  'p4':650,
  'p5':500,
  'p6':450,
  'p7':400,
  'p8':350,
  'p9':300,
  'p10':250,
}

test_cases={
  'p10':[5*i for i in range(max_sub['p10']/5,0,-1)],
  'p9':[5*i for i in range(max_sub['p9']/5,0,-1)],
  'p8':[5*i for i in range(max_sub['p8']/5-5,0,-1)],
  'p7':[5*i for i in range(max_sub['p7']/5,0,-1)],
  'p6':[5*i for i in range(max_sub['p6']/5-67,0,-1)],
  'p5':[5*i for i in range(max_sub['p5']/5-30,0,-1)],
  'p4':[5*i for i in range(max_sub['p4']/5,0,-1)],
  'p3':[5*i for i in range(max_sub['p3']/5-15,0,-1)],
  'p2':[5*i for i in range(max_sub['p2']/5,0,-1)],
  'p1':[5*i for i in range(max_sub['p1']/5,0,-1)],
}

def get_subscriber_distribution(sub,max_supported_system_rate):
  count=0
  sub_machines=''
  sub_distribution=''
  for machine in subscriber_test_machines:
    if count>=sub:
      break
    else:
      hw_type=get_hw_type(machine)
      num_sub=sub_per_hw_type['r%d'%(max_supported_system_rate)][hw_type]
      if num_sub>(sub-count):
        val=sub-count
      else:
        val=num_sub
        
      sub_distribution+='%s:t1:%d,'%(machine,val)
      sub_machines+='%s,'%(machine)
      count+=val

  return {'sub_machines':sub_machines.rstrip(','),
    'sub_distribution':sub_distribution.rstrip(','),
  }
  

def run(num_pub,num_sub,log_dir):
  print('executing test for num_pub:%d and num_sub:%d'%(num_pub,num_sub))
  #test params
  experiment_type='test'
  sleep_interval=100
  publication_rate=10
  payload=4000
  pub_sample_count=600
  max_supported_system_rate=100

  res=get_subscriber_distribution(num_sub,max_supported_system_rate)
  client_machines=res['sub_machines']
  sub_distribution=res['sub_distribution']
  
  #add client machines for publishers to clients
  client_machines+=',node3'
  sub_sample_count=pub_sample_count*num_pub

  conf="""run_id:%d
rbs:
ebs:node2
felbs:node1
clients:%s
topics:t1
no_subs:%d
no_pubs:%d
sub_distribution:%s
pub_distribution:node3:t1:%d
pub_sample_count:%d
sub_sample_count:%d
sleep_interval_milisec:%d
payload:%d"""%(num_sub,client_machines,num_sub,num_pub,
  sub_distribution,num_pub,pub_sample_count,
  sub_sample_count,sleep_interval,payload)
  print(conf+"\n")
  with open('conf/%s_conf'%(experiment_type),'w') as f:
    f.write(conf)

  #run experiment
  subprocess.check_call(['python','src/experiment.py',experiment_type,'conf/%s_conf'%(experiment_type),log_dir,'--kill'])
  #summarize results
  subprocess.check_call(['python','src/plots/summarize/summarize.py','-log_dir',log_dir,'-sub_dirs',str(num_sub)])

  time.sleep(5)

if __name__=="__main__":
  #pub_iterations=sorted([int(s[1:]) for s in test_cases.keys()],reverse=True)
  #for num_pub in pub_iterations[7:]:
  #  print('starting test for #publishers:%d'%(num_pub))
  #  sub_iterations=test_cases['p%d'%(num_pub)]
  #  print(sub_iterations)

  #  #create log directory 
  #  log_dir='/home/kharesp/log/distribution3_run2/p%d'%(num_pub)
  #  if not os.path.exists(log_dir):
  #    command_string='mkdir %s'%(log_dir)
  #    subprocess.check_call(['bash','-c',command_string])  

  #  for num_sub in sub_iterations:
  #run(num_pub,num_sub,log_dir)
  num_pub=1
  num_sub=1
  log_dir='/home/kharesp/log/p%d'%(num_pub)
  if not os.path.exists(log_dir):
    command_string='mkdir %s'%(log_dir)
    subprocess.check_call(['bash','-c',command_string])  
  run(num_pub,num_sub,log_dir)
