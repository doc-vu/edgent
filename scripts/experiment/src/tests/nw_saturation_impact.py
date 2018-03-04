import argparse,os,sys,time
from kazoo.client import KazooClient
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,experiment,infrastructure

def create_conf(sleep_interval_ms,payload,pub_sample_count):
  clients=['node3','node4','node5','node6','node7','node8','node9','node10','node11']
  conf="""run_id:2
rbs:
ebs:node3,node6,node9
felbs:node2
clients:node4,node5,node7,node8,node10,node11
topics:t1,t2,t3
no_subs:3
no_pubs:41
sub_distribution:node5:t1:1,node8:t2:1,node11:t3:1
pub_distribution:node4:t1:20,node7:t2:20,node10:t3:1
pub_sample_count:%d
sub_sample_count:%d
sleep_interval_milisec:%d
payload:%d"""%(pub_sample_count,pub_sample_count*20,
  sleep_interval_ms,payload)
  print(conf+"\n")
  with open('conf/conf','w') as f:
    f.write(conf)

def run(sleep_interval_ms,payload,pub_sample_count):
  create_conf(sleep_interval_ms,payload,pub_sample_count)
  infrastructure.Infrastructure('conf/conf').setup()
  zk=KazooClient(hosts=metadata.public_zk)
  zk.start()

  zk.create('/topics/t1','none')
  zk.create('/lb/topics/t1','none')
  zk.create('/topics/t2','none')
  zk.create('/lb/topics/t2','none')
  zk.create('/topics/t3','none')
  zk.create('/lb/topics/t3','none')

  zk.ensure_path('/eb/EB-30-10.20.30.3-0/t1')
  zk.ensure_path('/eb/EB-30-10.20.30.6-0/t2')
  zk.ensure_path('/eb/EB-30-10.20.30.9-0/t3')


  experiment.Experiment('conf/conf',True).run()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs #pub stress test')
  parser.add_argument('sleep_interval_ms',type=int,help='sleep interval for desired publication rate')
  parser.add_argument('payload',type=int,help='payload size in bytes')
  parser.add_argument('pub_sample_count',type=int,help='number of samples a publisher will send')
  args=parser.parse_args()

  run(args.sleep_interval_ms,args.payload,args.pub_sample_count)
