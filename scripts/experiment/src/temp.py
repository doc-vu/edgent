import argparse,os,sys,time,subprocess
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,util
from functools import reduce

#nw partition-1 
#subscriber_test_machines=['node17']
#publisher_test_machines=['node%d'%(i) for i in range(18,21,1)]

#nw partition-2
subscriber_test_machines=['node25']
publisher_test_machines=['node%d'%(i) for i in range(6,10,1)]

#nw partition-3
#subscriber_test_machines=['node12']
#publisher_test_machines=['node%d'%(i) for i in range(13,17,1)]


#maximum number of subscribers that can be created
maximum_subscribers=reduce(lambda x,y:x+y,\
  [metadata.sub_per_hw_type[metadata.get_hw_type(host)] for host in subscriber_test_machines])
print('Maximum #subscribers:%d'%(maximum_subscribers))

#maximum number of publishers that can be created
maximum_publishers=reduce(lambda x,y:x+y,\
  [metadata.pub_per_hw_type[metadata.get_hw_type(host)] for host in publisher_test_machines])
print('Maximum #publishers:%d'%(maximum_publishers))
print('Max #topics:%d'%(maximum_publishers/78))

print(78*8)
