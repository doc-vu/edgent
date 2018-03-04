from kazoo.client import KazooClient
import argparse

def check(exp,endpoint):
  zk=KazooClient('129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181')
  zk.start()
  endpoints=zk.get_children('/experiment/%s/%s'%(exp,endpoint))
  d={}
  for name in endpoints:
    parts=name.split('-')
    hostname=parts[2]
    if hostname in d:
      d[hostname]+=1
    else:
      d[hostname]=1
  for hostname in sorted(d.keys()):
    print('%s:%d\n'%(hostname,d[hostname]))
  zk.stop()
if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to get connected endpoints')
  parser.add_argument('exp',help='name of experiment run')
  parser.add_argument('endpointType',help='type of endpoint pub|sub')
  args=parser.parse_args()

  check(args.exp,args.endpointType)
