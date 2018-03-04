import argparse,os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,experiment,infrastructure

def create_conf(experiment_type,num_sub,num_pub,sleep_interval_ms,payload,pub_sample_count):
  num_clients=num_sub//metadata.max_subscribers_per_host
  if num_clients==0:
    clients=['node22']
    sub_distribution=['node22:t2:%d'%(num_sub)]
  else:
    clients=['node%d'%(22+i) for i in range(num_clients)]
    sub_distribution=['%s:t2:%d'%(cli,metadata.max_subscribers_per_host) for cli in clients ]
   
  #add client machines for publishers to clients
  clients.append('node5')
  sub_sample_count=pub_sample_count*num_pub
  conf="""run_id:%d
rbs:
ebs:node3
felbs:node1
clients:%s
topics:t2
no_subs:%d
no_pubs:%d
sub_distribution:%s
pub_distribution:node5:t2:%d
pub_sample_count:%d
sub_sample_count:%d
sleep_interval_milisec:%d
payload:%d"""%(num_sub,','.join(clients),num_sub,num_pub,
  ','.join(sub_distribution),num_pub,pub_sample_count,
  sub_sample_count,sleep_interval_ms,payload)
  print(conf+"\n")
  with open('conf/%s_conf'%(experiment_type),'w') as f:
    f.write(conf)

def run(experiment_type,min_sub,step_size,max_sub,setup,num_pub,
  sleep_interval_ms,payload,pub_sample_count,log_dir):
  #run test cases
  for num in range(min_sub, max_sub+step_size, step_size):
    create_conf(experiment_type,num,num_pub,sleep_interval_ms,payload,pub_sample_count)
    #check if infrastructure needs to be setup before every test
    if setup:
      print("Setting up Test Infrastructure")
      infrastructure.Infrastructure('conf/%s_conf'%(experiment_type)).setup()
      setup=False
    experiment.Experiment(experiment_type,'conf/%s_conf'%(experiment_type),log_dir,True).run()
    time.sleep(5)

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs #sub stress test')
  parser.add_argument('experiment_type',help='experiment type')
  parser.add_argument('min_sub',type=int,help='minimum number of subscribers to start from (multiple of max_subscribers_per_host)')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_sub to max_sub (multiple of max_subscribers_per_host)')
  parser.add_argument('max_sub',type=int,help='maximum number of subscribers (multiple of max_subscribers_per_host)')
  parser.add_argument('num_pub',type=int,help='number of publishers to launch')
  parser.add_argument('sleep_interval_ms',type=int,help='sleep interval for desired publication rate')
  parser.add_argument('payload',type=int,help='payload size in bytes')
  parser.add_argument('pub_sample_count',type=int,help='number of samples a publisher will send')
  parser.add_argument('log_dir',help='local log directory where logs after experiment run will be saved')
  parser.add_argument('--setup',action='store_true',help='flag to indicate infrastructure needs to be setup before testing')
  args=parser.parse_args()

  run(args.experiment_type,args.min_sub,args.step_size,args.max_sub,
    args.setup,args.num_pub,args.sleep_interval_ms,args.payload,args.pub_sample_count,args.log_dir)
