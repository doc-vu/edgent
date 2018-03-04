import argparse,os,sys,time,subprocess
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,util

brokers=['node%d'%(i) for i in range(2,7,1)]
publisher_test_machines=['node%d'%(i) for i in range(7,15,1)]
publisher_test_machines=publisher_test_machines + ['node27','node28','node29','node30','node31']
print(publisher_test_machines)
subscriber_test_machines=['node%d'%(i) for i in range(15,42,1)]
subscriber_test_machines.remove('node27')
subscriber_test_machines.remove('node28')
subscriber_test_machines.remove('node29')
subscriber_test_machines.remove('node30')
subscriber_test_machines.remove('node31')


pub_per_hw_type={'hw1':150, 'hw2':300}
sub_per_hw_type={'hw1':50,'hw2':100,'hw3':150,'hw4':200,'hw5':200}

#maximum number of subscribers that can be created
maximum_subscribers=reduce(lambda x,y:x+y,\
  [sub_per_hw_type[metadata.get_hw_type(host)] for host in subscriber_test_machines])
print('Maximum #subscribers:%d'%(maximum_subscribers))

#maximum number of publishers that can be created
maximum_publishers=reduce(lambda x,y:x+y,\
  [pub_per_hw_type[metadata.get_hw_type(host)] for host in publisher_test_machines])
print('Maximum #publishers:%d'%(maximum_publishers))
