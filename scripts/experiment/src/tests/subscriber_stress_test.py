import argparse,os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,experiment,infrastructure

def create_conf(num_sub):
  num_clients=num_sub//metadata.max_subscribers_per_host
  if num_clients==0:
    clients=['cli1-2']
    sub_distribution=['cli1-2:t1:%d'%(num_sub)]
  else:
    #clients=['cli1-%d'%(6*i+2) for i in range(num_clients)]
    clients=['cli1-%d'%(i+2) for i in range(48)]
    excluded_clients=['cli1-7','cli1-13','cli1-19','cli1-25','cli1-31','cli1-37','cli1-43','cli1-49']
    for cli in excluded_clients:
      clients.remove(cli)
    
    clients=clients[:num_clients]
    sub_distribution=['%s:t1:%d'%(cli,metadata.max_subscribers_per_host) for cli in clients ]
   
  #add client machines for publishers to clients
  clients.append('cli1-1')
  conf="""run_id:%d
rbs:
ebs:eb1
clients:%s
topics:t1
no_subs:%d
no_pubs:1
sub_distribution:%s
pub_distribution:cli1-1:t1:1
pub_sample_count:10000
sub_sample_count:10000
sleep_interval_milisec:10"""%(num_sub,','.join(clients),num_sub,','.join(sub_distribution))
  print(conf+"\n")
  with open('conf/conf','w') as f:
    f.write(conf)

def run(min_sub,step_size,max_sub,setup):
  #check if infrastructure needs to be setup before testing
  if setup:
    print("Setting up Test Infrastructure")
    infrastructure.Infrastructure('conf/conf').setup()
  
  #run test cases
  for num in range(min_sub,\
    max_sub+step_size,\
    step_size):
      create_conf(num)
      experiment.Experiment('conf/conf',True).run()
      time.sleep(5)

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs #sub stress test')
  parser.add_argument('min_sub',type=int,help='minimum number of subscribers to start from (multiple of max_subscribers_per_host)')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_sub to max_sub (multiple of max_subscribers_per_host)')
  parser.add_argument('max_sub',type=int,help='maximum number of subscribers (multiple of max_subscribers_per_host)')
  parser.add_argument('--setup',action='store_true',help='flag to indicate infrastructure needs to be setup before testing')
  args=parser.parse_args()

  run(args.min_sub,args.step_size,args.max_sub,args.setup)
