import argparse,os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,experiment,infrastructure

def create_conf(experiment_type,num_pub,num_sub,sleep_interval_ms,payload,pub_sample_count):
  num_clients=num_pub//metadata.max_publishers_per_host
  remainder=num_pub%metadata.max_publishers_per_host
  if num_clients==0:
    clients=['node20']
    pub_distribution=['node20:t1:%d'%(num_pub)]
  else:
    clients=['node%d'%(20+i) for i in range(num_clients)]
    pub_distribution=['%s:t1:%d'%(cli,metadata.max_publishers_per_host) for cli in clients ]
  if(remainder > 0 and num_clients>0):
    client='node%d'%(20+num_clients)
    clients.append(client)
    pub_distribution.append('%s:t1:%d'%(client,remainder))
   
  #add client machine for subscribers to clients
  clients.append('node26')
  sub_sample_count=pub_sample_count*num_pub
  conf="""run_id:%d
rbs:
ebs:node2
felbs:node1
clients:%s
topics:t1
no_subs:%d
no_pubs:%d
sub_distribution:node26:t1:%d
pub_distribution:%s
pub_sample_count:%d
sub_sample_count:%d
sleep_interval_milisec:%d
payload:%d"""%(num_pub,','.join(clients),num_sub,num_pub,
  num_sub,','.join(pub_distribution),pub_sample_count,
  sub_sample_count,sleep_interval_ms,payload)
  print(conf+"\n")
  with open('conf/%s_conf'%(experiment_type),'w') as f:
    f.write(conf)

def run(experiment_type,min_pub,step_size,max_pub,setup,num_sub,sleep_interval_ms,payload,pub_sample_count,log_dir):
  #run test cases
  for num in range(min_pub, max_pub+step_size, step_size):
    create_conf(experiment_type,num,num_sub,sleep_interval_ms,payload,pub_sample_count)
    #check if infrastructure needs to be setup before every test
    #if setup:
    #  print("Setting up Test Infrastructure")
    #  infrastructure.Infrastructure('conf/%s_conf'%(experiment_type)).setup()
    #  setup=False
    experiment.Experiment(experiment_type,'conf/%s_conf'%(experiment_type),log_dir,True).run()
    time.sleep(5)
    #summarize.summarize(log_dir,[num])
    

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs #pub stress test')
  parser.add_argument('experiment_type',help='experiment type')
  parser.add_argument('min_pub',type=int,help='minimum number of publishers to start from (multiple of max_publishers_per_host)')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_pub to max_pub (multiple of max_publishers_per_host)')
  parser.add_argument('max_pub',type=int,help='maximum number of publishers (multiple of max_publishers_per_host)')
  parser.add_argument('num_sub',type=int,help='number of subscribers to launch')
  parser.add_argument('sleep_interval_ms',type=int,help='sleep interval for desired publication rate')
  parser.add_argument('payload',type=int,help='payload size in bytes')
  parser.add_argument('pub_sample_count',type=int,help='number of samples a publisher will send')
  parser.add_argument('log_dir',help='local log directory where logs after experiment run will be saved')
  parser.add_argument('--setup',action='store_true',help='flag to indicate infrastructure needs to be setup before testing')
  args=parser.parse_args()

  run(args.experiment_type,args.min_pub,args.step_size,args.max_pub,
    args.setup,args.num_sub,args.sleep_interval_ms,args.payload,args.pub_sample_count,args.log_dir)
