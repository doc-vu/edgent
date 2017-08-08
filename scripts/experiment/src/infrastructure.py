import argparse,metadata,conf,subprocess,time

"""
Script that sets up the infrastructure before testing.
"""
class Infrastructure(object):
  def __init__(self,conf_file,teardown):
    #flag to determine whether to teardown existing infrastructure
    self.teardown=teardown
    #load test configuration
    self.conf=conf.Conf(conf_file)
    
  def setup(self):
    #clean up
    print("\n\n\nTearing down existing infrastructure")
    self.clean()

    #Launch infrastructure components after tearing down
    print("\n\n\nSetting up infrastructure processes")
    self.setup_infrastructure()


  def clean(self):
    #kill existing edge-broker processes
    print("\n\n\nKilling all existing processes on brokers")
    self.kill_existing_processes()

    #clear logs
    print("\n\n\nCleaning log directory on brokers")
    self.clean_logs()

  def kill_existing_processes(self):
    #kill existing broker processes on brokers
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml  --limit %s\
      --extra-vars="pattern=Broker"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])

  def clean_logs(self):
    command_string='cd %s && ansible-playbook playbooks/util/clean.yml \
      --extra-vars="dir=/home/ubuntu/infrastructure_log/" --limit %s'\
      %(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

  def setup_infrastructure(self):
    #ensure netem rules are set on RBs

    #start EdgeBroker on ebs
    print("\n\n\nStarting broker processes on edge-brokers")
    command_string='cd %s && ansible-playbook playbooks/experiment/eb.yml  --limit %s'%\
      (metadata.ansible,','.join(self.conf.ebs))
    subprocess.check_call(['bash','-c',command_string])
    
    #start RoutingBroker on rbs


if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for setting up edgent infrastructure nodes')
  parser.add_argument('conf',help='configuration file containing setup information')
  args=parser.parse_args()
  Infrastructure(args.conf).setup()
