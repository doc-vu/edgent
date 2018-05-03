import zmq,json,socket,subprocess

workspace='/home/ubuntu/workspace/edgent'
hostname_alias={
  'isislab30':'node17',
  'isislab25':'node18',
  'isislab24':'node19',
  'isislab23':'node20',
  'isislab22':'node21',
  'isislab21':'node22',
  'isislab26':'node23',
  'isislab27':'node24',
  'isislab28':'node25',
  'isislab29':'node26',
  'isislab19':'node27',
  'isislab17':'node28',
  'isislab15':'node29',
  'isislab14':'node30',
  'isislab13':'node31',
  'isislab12':'node32',
  'isislab11':'node33',
  'isislab10':'node34',
  'isislab9':'node35',
  'isislab8':'node36',
  'isislab7':'node37',
  'isislab6':'node38',
  'isislab5':'node39',
  'isislab2':'node40',
  'isislab1':'node41',
}

hostname=socket.gethostname()
if hostname in hostname_alias:
  hostname=hostname_alias[hostname]

context= zmq.Context(1)
sub= context.socket(zmq.SUB)
sub.setsockopt_string(zmq.SUBSCRIBE,u'')
sub.connect('tcp://10.20.30.1:1026')


while True:
  msg=sub.recv()
  data= json.loads(msg.decode('utf-8'))
  endpoint_type= data.get('endpoint')
  d= data.get('topic_descriptions')
  
  if hostname in d:
      if (endpoint_type=='pub'):
        experiment_type= data.get('experiment_type')
        zk_connector= data.get('zk_connector')
        command_string='.%s/scripts/start_publishers2.sh %s %s %s'%(workspace,d[hostname],zk_connector,experiment_type) 
        print(command_string)
        #subprocess.check_call(['bash','-c',command_string])

      elif (endpoint_type=='sub'):
        run_id=data.get('run_id')
        out_dir=data.get('out_dir')
        experiment_type=data.get('experiment_type')
        command_string='.%s/scripts/start_subscribers2.sh %s %s %s %s'%(workspace,d[hostname],run_id,out_dir,experiment_type)
        print(command_string)
        #subprocess.check_call(['bash','-c',command_string])
