import argparse,os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,experiment,infrastructure

def create_conf(num_sub,num_pub):
  num_clients=num_sub//metadata.max_subscribers_per_host
  if num_clients==0:
    clients=['node6']
    sub_distribution=['node6:t1:%d'%(num_sub)]
  else:
    clients=['node%d'%(6+i) for i in range(num_clients)]
    sub_distribution=['%s:t1:%d'%(cli,metadata.max_subscribers_per_host) for cli in clients ]
   
  #add client machines for publishers to clients
  clients.append('node5')
  sub_sample_count=10000
  pub_sample_count=sub_sample_count/num_pub
  conf="""run_id:%d
rbs:
ebs:node3,node4
felbs:node2
clients:%s
topics:t1
no_subs:%d
no_pubs:%d
sub_distribution:%s
pub_distribution:node5:t1:%d
pub_sample_count:%d
sub_sample_count:%d
sleep_interval_milisec:10"""%(num_sub,','.join(clients),num_sub,num_pub,
  ','.join(sub_distribution),num_pub,pub_sample_count,sub_sample_count)
  print(conf+"\n")
  #with open('conf/conf','w') as f:
  #  f.write(conf)

def run(min_sub,step_size,max_sub,setup,num_pub):
  #run test cases
  for num in range(min_sub, max_sub+step_size, step_size):
    create_conf(num,num_pub)
    #check if infrastructure needs to be setup before testing
    #if setup:
    #  print("Setting up Test Infrastructure")
    #  infrastructure.Infrastructure('conf/conf').setup()
    #  setup=False
    #experiment.Experiment('conf/conf',True).run()
    #time.sleep(5)

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs #sub stress test for allSub replication scheme')
  parser.add_argument('min_sub',type=int,help='minimum number of subscribers to start from (multiple of max_subscribers_per_host)')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_sub to max_sub (multiple of max_subscribers_per_host)')
  parser.add_argument('max_sub',type=int,help='maximum number of subscribers (multiple of max_subscribers_per_host)')
  parser.add_argument('num_pub',type=int,help='number of publishers to launch')
  parser.add_argument('--setup',action='store_true',help='flag to indicate infrastructure needs to be setup before testing')
  args=parser.parse_args()

  run(args.min_sub,args.step_size,args.max_sub,args.setup,args.num_pub)
