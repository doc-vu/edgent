import argparse,os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,experiment,infrastructure

#nodes for hosting publishers
type_1_pub_nodes=['node5','node6','node7','node8','node9','node10']
type_2_pub_nodes=['node17','node18','node19']

#nodes for hosting subscribers
type_1_sub_nodes=['node11','node12','node13','node14','node15','node16']
type_2_sub_nodes=['node20','node21','node22']


def create_conf(num_topics,num_endpoints,sleep_interval_ms,payload,pub_sample_count):
  num_clients_t1=num_topics//metadata.max_topics_per_host_type1
  if (num_clients_t1>len(type_1_pub_nodes)):
    num_clients_t1=len(type_1_pub_nodes)
  if num_clients_t1==0:
    clients=[type_1_pub_nodes[0],type_1_sub_nodes[0]]
    pub_distribution=['%s:t%d:%d'%(type_1_pub_nodes[0],topic+1,num_endpoints) for topic in range(num_topics)]
    sub_distribution=['%s:t%d:%d'%(type_1_sub_nodes[0],topic+1,num_endpoints) for topic in range(num_topics)]
  else:
    clients=['%s,%s'%(type_1_pub_nodes[i],type_1_sub_nodes[i]) for i in range(num_clients_t1)]
    pub_distribution=['%s:t%d:%d'%(type_1_pub_nodes[idx],
      idx*metadata.max_topics_per_host_type1+topic+1,num_endpoints) for topic in range(metadata.max_topics_per_host_type1) for idx in range(num_clients_t1) ]
    sub_distribution=['%s:t%d:%d'%(type_1_sub_nodes[idx],
      idx*metadata.max_topics_per_host_type1+topic+1,num_endpoints) for topic in range(metadata.max_topics_per_host_type1) for idx in range(num_clients_t1) ]
  
  remaining=(num_topics-num_clients_t1*metadata.max_topics_per_host_type1) if (num_clients_t1>0) else 0
  if(remaining > 0):
    num_clients_t2=remaining//metadata.max_topics_per_host_type2
    remainder=remaining%metadata.max_topics_per_host_type2
 
    topics_on_type1=num_clients_t1*metadata.max_topics_per_host_type1
    if(num_clients_t2>len(type_2_pub_nodes)):
      num_clients_t2=len(type_2_pub_nodes)
    if(num_clients_t2==0):
      clients.append(type_2_pub_nodes[0])
      clients.append(type_2_sub_nodes[0])
      for t in range(remainder):
        pub_distribution.append('%s:t%d:%d'%(type_2_pub_nodes[0],topics_on_type1+t+1,num_endpoints))
        sub_distribution.append('%s:t%d:%d'%(type_2_sub_nodes[0],topics_on_type1+t+1,num_endpoints))
    else:
      for machine in range(num_clients_t2):
        clients.append('%s,%s'%(type_2_pub_nodes[machine],type_2_sub_nodes[machine]))
        for t in range(metadata.max_topics_per_host_type2):
          pub_distribution.append('%s:t%d:%d'%(type_2_pub_nodes[machine],topics_on_type1+metadata.max_topics_per_host_type2*machine+t+1,num_endpoints))
          sub_distribution.append('%s:t%d:%d'%(type_2_sub_nodes[machine],topics_on_type1+metadata.max_topics_per_host_type2*machine+t+1,num_endpoints))

    topics_on_type2=(num_clients_t2*metadata.max_topics_per_host_type2) if (num_clients_t2>0) else remainder
    if (remainder>0 and num_clients_t2>0 and num_clients_t2<len(type_2_pub_nodes)):
      clients.append('%s,%s'%(type_2_pub_nodes[num_clients_t2],type_2_sub_nodes[num_clients_t2]))
      for t in range(remainder):
        pub_distribution.append('%s:t%d:%d'%(type_2_pub_nodes[num_clients_t2],topics_on_type1+topics_on_type2+t+1,num_endpoints)) 
        sub_distribution.append('%s:t%d:%d'%(type_2_sub_nodes[num_clients_t2],topics_on_type1+topics_on_type2+t+1,num_endpoints))

  sub_sample_count=pub_sample_count*num_endpoints
  conf="""run_id:%d
rbs:
ebs:node3
felbs:node2
clients:%s
topics:%s
no_subs:%d
no_pubs:%d
sub_distribution:%s
pub_distribution:%s
pub_sample_count:%d
sub_sample_count:%d
sleep_interval_milisec:%d
payload:%d"""%(num_topics,','.join(clients),
  ','.join(['t%d'%(num+1) for num in range(num_topics)]),
  num_topics*num_endpoints,num_topics*num_endpoints,
  ','.join(sub_distribution),','.join(pub_distribution),
  pub_sample_count,
  sub_sample_count,
  sleep_interval_ms,
  payload)
  
  print(conf+"\n")
  with open('conf/conf','w') as f:
    f.write(conf)

def run(min_topics,step_size,max_topics,setup,num_endpoints,
  sleep_interval_ms,payload,pub_sample_count):
  #run test cases
  for num in range(min_topics, max_topics+step_size, step_size):
    create_conf(num,num_endpoints,sleep_interval_ms,payload,pub_sample_count)
    #check if infrastructure needs to be setup before testing
    if setup:
      print("Setting up Test Infrastructure")
      infrastructure.Infrastructure('conf/conf').setup()
      #setup=False
    experiment.Experiment('conf/conf',True).run()
    time.sleep(15)

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting topic stress test')
  parser.add_argument('min_topics',type=int,help='minimum number of topics to start from')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_topics to max_topics')
  parser.add_argument('max_topics',type=int,help='maximum number of topics to test')
  parser.add_argument('num_endpoints',type=int,help='number of publishers and subscribers per topic')
  parser.add_argument('sleep_interval_ms',type=int,help='sleep interval for desired publication rate')
  parser.add_argument('payload',type=int,help='payload size in bytes')
  parser.add_argument('pub_sample_count',type=int,help='number of samples a publisher will send')
  parser.add_argument('--setup',action='store_true',help='flag to indicate infrastructure needs to be setup before testing')

  args=parser.parse_args()
  run(args.min_topics,args.step_size,args.max_topics,args.setup,args.num_endpoints,
    args.sleep_interval_ms,args.payload,args.pub_sample_count)
