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
    'isislab30':'eno4',
    'isislab25':'eno4',
    'isislab24':'eno4',
    'isislab23':'eno4',
    'isislab22':'eno4',
    'isislab21':'eno4',
    'isislab26':'eno4',
    'isislab27':'eno4',
    'isislab28':'eno4',
    'isislab29':'eno4',
    'isislab19':'em2',
    'isislab17':'em2',
    'isislab15':'em2',
    'isislab14':'em2',
    'isislab13':'em2',
    'isislab12':'em2',
    'isislab11':'em2',
    'isislab10':'em2',
    'isislab9':'em2',
    'isislab8':'em2',
    'isislab7':'em2',
    'isislab6':'em2',
    'isislab5':'em2',
    'isislab2':'em2',
    'isislab1':'em2',
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
