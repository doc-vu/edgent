import os,argparse

def filter(log_dir,run_id):
  hostname_iface={
    'node1':'enp13s0f1',
    'node2':'enp13s0f1',
    'node3':'enp13s0f0',
    'node4':'enp12s0f1',
    'node5':'enp13s0f0',
    'node6':'enp13s0f1',
    'node7':'enp12s0f1',
    'node8':'enp13s0f1',
    'node9':'enp13s0f1',
    'node10':'enp15s0f1',
    'node11':'enp15s0f1',
    'node12':'enp13s0f1',
    'node13':'enp13s0f1',
    'node14':'enp13s0f1',
    'node15':'enp13s0f0',
    'node16':'enp13s0f1',
    'node17':'eno4',
    'node18':'eno4',
    'node19':'eno4',
    'node20':'eno4',
    'node21':'eno4',
    'node22':'eno4',
    'node23':'eno4',
    'node24':'eno4',
    'node25':'eno4',
    'node26':'eno4',
    'node27':'em2',
    'node28':'em2',
    'node29':'em2',
    'node30':'em2',
    'node31':'em2',
    'node32':'em2',
    'node33':'em2',
    'node34':'em2',
    'node35':'em2',
    'node36':'em2',
    'node37':'em2',
    'node38':'em2',
    'node39':'em2',
    'node40':'em2',
    'node41':'em2',
  }

  for f in os.listdir('%s/%s'%(log_dir,run_id)):
    if f.startswith('nw'):
      hostname=f.split('_')[1]
      if hostname in hostname_iface:
        iface=hostname_iface[hostname]
        input_file='%s/%s/%s'%(log_dir,run_id,f)
        output_file='%s/%s/nw_filtered_%s'%(log_dir,run_id,f.partition('_')[2])
        with open(input_file,'r') as inp, open(output_file,'w') as out:
          for line in inp:
            contents=line.split(";")
            ts=contents[2]
            ifaces=contents[3:]
            if iface in ifaces:
              idx=ifaces.index(iface)
              rx,tx=ifaces[idx+3:idx+5]
              out.write('%s,%s,%s,%s\n'%(ts,iface,rx,tx))

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for filtering out default network interface\'s utilization data')
  parser.add_argument('log_dir',help='log directory in which data is stored')
  parser.add_argument('run_id',help='experiment run id. The network file to be filtered must be in log/run_id/')
  args=parser.parse_args()
  filter(args.log_dir,args.run_id)
