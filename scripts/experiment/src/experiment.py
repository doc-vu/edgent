import argparse,subprocess,metadata,time,os,conf,json

class Experiment(object):
  def __init__(self,conf_file,run_id,kill):
    #load the test configuration 
    self.conf=conf.Conf(conf_file)
    self.run_id=run_id
    #flag to determine whether to kill pre-existing client endpoints
    self.kill=kill
    
  def run(self):
    #clean up
    self.clean()

    #launch subscribers
    print("\n\n\nStarting subscriber processes")
    self.start_subscribers()

    #launch publishers
    print("Starting publisher processes")
    self.start_publishers()

    #collect logs
    print("\n\n\nCollecting logs")
    self.collect_logs()

  def clean(self):
    #kill existing client processes
    if(self.kill):
      print("\n\n\nKilling all client processes")
      self.kill_existing_processes()

    #clear logs
    print("\n\n\nCleaning log directory on all host machines")
    self.clean_logs()
    
    
  def kill_existing_processes(self):
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

   
  def start_subscribers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.subscribers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.subscribers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.run_id])
    
    pairs={k:self.conf.subscribers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.run_id])

  def start_publishers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.publishers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.publishers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),str(self.conf.sleep_interval),self.run_id])
    
    pairs={k:self.conf.publishers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),str(self.conf.sleep_interval),self.run_id])

  def collect_logs(self):
    #ensure local log directory exists
    dest_dir='%s'%(metadata.local_log_dir)
    if not os.path.exists(dest_dir):
      os.makedirs(dest_dir)

    #fetch logs from all hosts
    command_string='cd %s && ansible-playbook playbooks/util/copy.yml  --limit %s\
      --extra-vars="src_dir=%s dest_dir=%s"'%(metadata.ansible,
      self.conf.hosts,
      '%s/%s'%(metadata.remote_log_dir,self.run_id), 
      dest_dir)
    subprocess.check_call(['bash','-c',command_string])
    
if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for running an experiment')
  parser.add_argument('conf',help='configuration file containing experiment setup information')
  parser.add_argument('run_id',help='run id of this experiment')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to specify whether to kill pre-existing client processes')
  args=parser.parse_args()

  Experiment(args.conf,args.run_id,args.kill).collect_logs()
