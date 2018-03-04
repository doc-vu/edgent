import argparse,subprocess,metadata,time,os,conf,json,zk,time

class Experiment(object):
  def __init__(self,experiment_type,conf_file,log_dir,kill):
    #type of experiment
    self.experiment_type=experiment_type
    #load the test configuration 
    self.conf=conf.Conf(conf_file)
    #local log directory where collected logs after experiment run will be stored
    self.log_dir=log_dir
    #flag to determine whether to kill pre-existing client endpoints
    self.kill=kill
    #zk communication
    self.zk=zk.Zk(self.experiment_type,self.conf.run_id,self.conf)
    
  def run(self):
    #Re-start EdgeBroker on ebs
    print("\n\n\nRe-starting broker processes on edge-brokers")
    command_string='cd %s && ansible-playbook playbooks/experiment/eb.yml  --limit %s\
      --extra-vars="zk_connector=%s io_threads=%d"'%\
      (metadata.ansible,','.join(self.conf.ebs),metadata.public_zk,metadata.io_threads)
    subprocess.check_call(['bash','-c',command_string])

    #clean up
    self.clean()

    #setup zk paths,barriers and watches for this experiment run
    print("\n\n\nSetting up zk tree for coordination")
    self.zk.setup()

    
    #ensure netem rules are set on client machines
    #print("\n\n\nEnsure netem rules are set on clients")
    #self.netem()

    #start monitoring processes
    print("\n\n\nStarting monitor processes")
    self.start_monitors()

    #launch subscribers
    print("\n\n\nStarting subscriber processes")
    self.start_subscribers()

    #wait for all subscribers to join
    print("\n\n\nWaiting on subscriber barrier, until all subscribers have joined")
    self.zk.wait('subscriber')

    #launch publishers
    print("Starting publisher processes")
    self.start_publishers()
  
    #wait for experiment to finish
    print("\n\n\nWaiting on finished barrier, until all subscribers have exited")
    self.zk.wait('finished')

    #wait for monitoring processes to exit before collecting logs
    print("\n\n\nAll subscribers have exited. Will wait for monitors to exit, before collecting logs")
    self.zk.wait('logs')
    
    ##filter out network statistics for default nw interface
    #print("\n\n\nFiltering network statistics for default nw interface")
    #self.filter_nw_interface()
    
    #wait for sometime before collecting logs after an experiment run
    time.sleep(5)

    #collect logs
    print("\n\n\nCollecting logs")
    self.collect_logs()

    ##ensure all logs are collected
    #time.sleep(5)
    #self.collect_logs()

    #copy test configuration
    self.copy_conf()

    #kill publisher endpoints after test has finished execution
    self.kill_publishers()
   
    #delete experiment path 
    self.zk.clean()

    #exit
    self.zk.stop()

  def clean(self):
    #kill existing client and monitoring processes
    #if(self.kill):
    #  print("\n\n\nKilling all monitoring and client processes")
    #  self.kill_existing_processes()

    #clean zk tree
    print("\n\n\nCleaning up zk tree: /experiment path")
    self.zk.clean()

    #clear logs
    print("\n\n\nCleaning log directory on all host machines")
    self.clean_logs()

    #free cached memory
    print("\n\n\nCleaning cached memory on all host machines")
    self.clean_memory()
    
    
  def kill_existing_processes(self):
    #kill existing monitoring processes on all brokers and clients
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml -f 16 --limit %s\
      --extra-vars="pattern=Monitor,edgent.endpoints"'%(metadata.ansible,self.conf.brokers_and_clients)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing publishers and subscriber processes on clients
    #command_string='cd %s && ansible-playbook playbooks/util/kill.yml -f 16 \
    #  --limit %s --extra-vars="pattern=edgent.endpoints"'%(metadata.ansible,','.join(self.conf.clients))
    #subprocess.check_call(['bash','-c', command_string])


  def clean_logs(self):
    #clean experiment logs from all broker and client machines
    command_string='cd %s && ansible-playbook playbooks/util/clean.yml -f 16\
      --extra-vars="dir=/home/ubuntu/log/"  --limit %s'\
      %(metadata.ansible,self.conf.brokers_and_clients)
    subprocess.check_call(['bash','-c',command_string])

  def clean_memory(self):
    #clean cached memory on all brokers
    command_string='cd %s && ansible-playbook playbooks/experiment/clean_memory.yml  -f 16 --limit %s'%\
      (metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

  def netem(self):
    command_string='cd %s && ansible-playbook playbooks/experiment/netem_cli.yml -f 16 --limit %s'%\
      (metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c',command_string])

  def start_monitors(self):
    #start Monitors on all brokers
    command_string='cd %s && ansible-playbook playbooks/experiment/monitor.yml -f 16 \
      --extra-vars="run_id=%s zk_connector=%s pub_node=0 experiment_type=%s core_count=%d" --limit %s'%\
      (metadata.ansible,self.conf.run_id,metadata.public_zk,self.experiment_type,metadata.restricted_core_count,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

    ##start Monitors on all subscriber nodes 
    command_string='cd %s && ansible-playbook playbooks/experiment/monitor.yml -f 16\
      --extra-vars="run_id=%s zk_connector=%s pub_node=0 experiment_type=%s core_count=-1" --limit %s'%\
      (metadata.ansible,self.conf.run_id,metadata.public_zk,self.experiment_type,','.join(self.conf.subscriber_client_machines))
    subprocess.check_call(['bash','-c',command_string])

    ##start Monitors on all publisher nodes
    command_string='cd %s && ansible-playbook playbooks/experiment/monitor.yml -f 16\
      --extra-vars="run_id=%s zk_connector=%s pub_node=1 experiment_type=%s core_count=-1" --limit %s'%\
      (metadata.ansible,self.conf.run_id,metadata.public_zk,self.experiment_type,','.join(self.conf.publisher_client_machines))
    subprocess.check_call(['bash','-c',command_string])
   
  def start_subscribers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.subscribers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.subscribers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.conf.run_id,self.experiment_type])
    
    pairs={k:self.conf.subscribers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.conf.run_id,self.experiment_type])

  def start_publishers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.publishers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.publishers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),\
        str(self.conf.sleep_interval_milisec),self.conf.run_id,str(self.conf.payload),self.experiment_type])
    
    pairs={k:self.conf.publishers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),\
      str(self.conf.sleep_interval_milisec),self.conf.run_id,str(self.conf.payload),self.experiment_type])

  #def filter_nw_interface(self):
  #  command_string='cd %s && ansible-playbook playbooks/experiment/filter_nw_interface.yml \
  #    --extra-vars="run_id=%s" --limit %s'%\
  #    (metadata.ansible,self.conf.run_id,self.conf.brokers_and_clients)
  #  subprocess.check_call(['bash','-c',command_string])

  def collect_logs(self):
    #ensure local log directory exists
    dest_dir='%s'%(self.log_dir)
    if not os.path.exists(dest_dir):
      os.makedirs(dest_dir)

    #fetch logs from all hosts
    command_string='cd %s && ansible-playbook playbooks/util/copy.yml  --limit %s\
      --extra-vars="src_dir=%s dest_dir=%s"'%(metadata.ansible,
      self.conf.brokers_and_clients,
      '%s/%s'%(metadata.remote_log_dir,self.conf.run_id), 
      dest_dir)
    subprocess.check_call(['bash','-c',command_string])

  def copy_conf(self):
    command_string='cp conf/%s_conf %s/%s/conf'%(self.experiment_type,self.log_dir,self.conf.run_id)                
    subprocess.check_call(['bash','-c',command_string]) 
 
  def kill_publishers(self):
    #kill publishers after test has finished
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml -f 16 \
      --limit %s --extra-vars="pattern=edgent.endpoints"'%(metadata.ansible,','.join(self.conf.publisher_client_machines))
    subprocess.check_call(['bash','-c', command_string])

    
if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for running an experiment')
  parser.add_argument('exp_type',help='type of experiment')
  parser.add_argument('conf',help='configuration file containing experiment setup information')
  parser.add_argument('log_dir',help='path to local log directory where logs after experiment run will be saved')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to specify whether to kill pre-existing client processes')
  args=parser.parse_args()

  Experiment(args.exp_type,args.conf,args.log_dir,args.kill).run()
