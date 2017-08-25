import argparse,metadata,conf,subprocess,time
from kazoo.client import KazooClient

"""
Script that sets up the infrastructure before testing.
"""
class Infrastructure(object):
  def __init__(self,conf_file):
    #load test configuration
    self.conf=conf.Conf(conf_file)
    self._zk=KazooClient(hosts=metadata.public_zk)
    self._zk.start()
    
  def setup(self):
    #clean up
    print("\n\n\nTearing down existing infrastructure")
    self.clean()

    #setup zk tree
    self.setup_zk()

    #Launch infrastructure components after tearing down
    print("\n\n\nSetting up infrastructure processes")
    self.setup_infrastructure()

    self._zk.stop()

  def clean(self):
    #kill existing edge-broker processes
    print("\n\n\nKilling all existing FE, LB, Broker processes on infrastructure nodes")
    self.kill_existing_processes()

    #clear logs
    print("\n\n\nCleaning log directory on infrastructure nodes")
    self.clean_logs()

  def kill_existing_processes(self):
    #kill infrastructure processes on infrastructure nodes
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml  --limit %s\
      --extra-vars="pattern=Broker,LoadBalancer,Frontend"'%\
      (metadata.ansible,self.conf.infrastructure)
    subprocess.check_call(['bash','-c', command_string])

  def clean_logs(self):
    command_string='cd %s && ansible-playbook playbooks/util/clean.yml \
      --extra-vars="dir=/home/ubuntu/infrastructure_log/" --limit %s'\
      %(metadata.ansible,self.conf.infrastructure)
    subprocess.check_call(['bash','-c',command_string])

  def setup_zk(self):
    #delete pre-existing sub-trees under /eb and /topics
    if self._zk.exists(metadata.topics_path):
      self._zk.delete(metadata.topics_path,recursive=True)
    if self._zk.exists(metadata.eb_path):
      self._zk.delete(metadata.eb_path,recursive=True)

    #create /eb and /topics path
    self._zk.ensure_path(metadata.topics_path) 
    self._zk.ensure_path(metadata.eb_path) 

  def setup_infrastructure(self):
    #ensure netem rules are set on RBs

    #start EdgeBroker on ebs
    print("\n\n\nStarting broker processes on edge-brokers")
    command_string='cd %s && ansible-playbook playbooks/experiment/eb.yml  --limit %s\
      --extra-vars="zk_connector=%s io_threads=%d"'%\
      (metadata.ansible,','.join(self.conf.ebs),metadata.zk,metadata.io_threads)
    subprocess.check_call(['bash','-c',command_string])

    #start LB on felb nodes
    print("\n\n\nStarting LoadBalancer processes on felb nodes")
    command_string='cd %s && ansible-playbook playbooks/experiment/lb.yml  --limit %s\
      --extra-vars="zk_connector=%s"'%\
      (metadata.ansible,','.join(self.conf.felbs),metadata.zk)
    subprocess.check_call(['bash','-c',command_string])

    #start FE on felb nodes
    print("\n\n\nStarting Frontend processes on felb nodes")
    command_string='cd %s && ansible-playbook playbooks/experiment/fe.yml  --limit %s\
      --extra-vars="zk_connector=%s"'%\
      (metadata.ansible,','.join(self.conf.felbs),metadata.zk)
    subprocess.check_call(['bash','-c',command_string])
    
    #start RoutingBroker on rbs


if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for setting up edgent infrastructure nodes')
  parser.add_argument('conf',help='configuration file containing setup information')
  args=parser.parse_args()
  Infrastructure(args.conf).setup()
