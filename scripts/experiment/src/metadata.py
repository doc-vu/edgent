#zk='10.20.30.1:2181,10.20.30.15:2181,10.20.30.16:2181'
public_zk='129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181'
public_zk2='129.59.107.134:2181'
public_zk3='129.59.107.57:2181'
public_zk4='129.59.107.201:2181'
ansible='/home/kharesp/workspace/java/edgent/scripts/ansible'
remote_log_dir='/home/ubuntu/log'
local_log_dir='/home/kharesp/log'
experiment_path='/experiment'
topics_path='/topics'
eb_path='/eb'
topic_level_lb_path='/lb/topics'
max_subscribers_per_host=500
max_publishers_per_host=1000
type_1_hosts=['node1','node2','node3','node4','node5','node6','node7','node8','node9','node10','node11','node12','node13','node14','node15','node16']
type_2_hosts=['node17','node18','node19','node20','node21','node22']
max_topics_per_host_type1=30
max_topics_per_host_type2=60
initial_samples=0
initial_samples_per_pub=24
payload_size=4000


#change these settings for EB when EB is restricted to run on only 1 or 2 core(s)
io_threads=3
#restricted core count=-1 implies all available cores are being used by the EB
restricted_core_count=-1

#description of cluster hardware
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

pub_per_hw_type={'hw1':109,'hw2':180,'hw3':220,'hw4':220,'hw5':190}
sub_per_hw_type={'hw1':10,'hw2':20,'hw3':35,'hw4':45,'hw5':45}

def get_host_subscriber_capacity(host):
  return sub_per_hw_type[get_hw_type(host)]

def get_host_publisher_capacity(host):
  return pub_per_hw_type[get_hw_type(host)]

maximum_nw_util_gb=.8

per_publisher_publication_rates=[1,5,10]
#aggregate_publication_rates= range(10,110,10)
aggregate_publication_rates= [10,20,40]
publisher_distributions={
  'r10': ['10:1','5:1,1:5','1:10'],
  'r20': ['20:1','10:1,2:5','2:10'],
  'r30': ['30:1','10:1,4:5','3:10'],
  'r40': ['40:1','15:1,5:5','4:10'],
  'r50': ['50:1','20:1,6:5','5:10'],
  'r60': ['60:1','20:1,8:5','6:10'],
  'r70': ['70:1','25:1,9:5','7:10'],
  'r80': ['80:1','30:1,10:5','8:10'],
  'r90': ['90:1','30:1,12:5','9:10'],
  'r100': ['100:1','35:1,13:5','10:10'],
}
subscription_sizes= range(10,210,10)
payload_sizes=range(100,4100,100)
