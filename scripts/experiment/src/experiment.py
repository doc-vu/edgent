import argparse,subprocess,metadata,time,os,conf,json,zk

class Experiment(object):
  def __init__(self,conf_file,kill):
    #load the test configuration 
    self.conf=conf.Conf(conf_file)
    #flag to determine whether to kill pre-existing client endpoints
    self.kill=kill
    #zk communication
    self.zk=zk.Zk(self.conf.run_id,self.conf)
    
  def run(self):
    #clean up
    self.clean()

    #setup zk paths,barriers and watches for this experiment run
    print("\n\n\nSetting up zk tree for coordination")
    self.zk.setup()
    
    #ensure netem rules are set on client machines
    print("\n\n\nEnsure netem rules are set on clients")
    self.netem()

    #start monitoring processes
    print("\n\n\nStarting monitor processes on brokers")
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
    self.zk.wait('logs')

    #collect logs
    print("\n\n\nCollecting logs")
    self.collect_logs()

    #exit
    self.zk.stop()

  def clean(self):
    #kill existing client and monitoring processes
    if(self.kill):
      print("\n\n\nKilling all monitoring and client processes")
      self.kill_existing_processes()

    #clean zk tree
    print("\n\n\nCleaning up zk tree: /experiment path")
    self.zk.clean()

    #clear logs
    print("\n\n\nCleaning log directory on all host machines")
    self.clean_logs()
    
    
  def kill_existing_processes(self):
    #kill existing monitoring processes on brokers
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml  --limit %s\
      --extra-vars="pattern=Monitor"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing publishers and subscriber processes on clients
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml  --limit %s\
      --extra-vars="pattern=edgent.clients."'%\
      (metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c', command_string])


  def clean_logs(self):
    #clean experiment logs from all host machines
    command_string='cd %s && ansible-playbook playbooks/util/clean.yml\
      --extra-vars="dir=/home/ubuntu/log/"  --limit %s'\
      %(metadata.ansible,self.conf.hosts)
    subprocess.check_call(['bash','-c',command_string])

  def start_monitors(self):
    #start Monitors on brokers
    command_string='cd %s && ansible-playbook playbooks/experiment/monitor.yml \
      --extra-vars="run_id=%s zk_connector=%s" --limit %s'%\
      (metadata.ansible,self.conf.run_id,metadata.zk,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

  def netem(self):
    command_string='cd %s && ansible-playbook playbooks/experiment/netem_cli.yml  --limit %s'%\
      (metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c',command_string])
   
  def start_subscribers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.subscribers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.subscribers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.conf.run_id])
    
    pairs={k:self.conf.subscribers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.conf.run_id])

  def start_publishers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.publishers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.publishers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),str(self.conf.sleep_interval_milisec),self.conf.run_id])
    
    pairs={k:self.conf.publishers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),str(self.conf.sleep_interval_milisec),self.conf.run_id])

  def collect_logs(self):
    #ensure local log directory exists
    dest_dir='%s'%(metadata.local_log_dir)
    if not os.path.exists(dest_dir):
      os.makedirs(dest_dir)

    #fetch logs from all hosts
    command_string='cd %s && ansible-playbook playbooks/util/copy.yml  --limit %s\
      --extra-vars="src_dir=%s dest_dir=%s"'%(metadata.ansible,
      self.conf.hosts,
      '%s/%s'%(metadata.remote_log_dir,self.conf.run_id), 
      dest_dir)
    subprocess.check_call(['bash','-c',command_string])
    
if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for running an experiment')
  parser.add_argument('conf',help='configuration file containing experiment setup information')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to specify whether to kill pre-existing client processes')
  args=parser.parse_args()

  Experiment(args.conf,args.kill).run()
