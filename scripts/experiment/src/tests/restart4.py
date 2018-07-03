import threading,time,sys,os,signal,subprocess,json
from kazoo.client import KazooClient
from kazoo.retry import RetryFailedError
from kazoo.exceptions import KazooException
import rate_processing_interval_combinations4
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,send_mail

class Hawk(object):
  def __init__(self,config,log_dir,run_id,zk_connector,fe_address):
    self.config=config
    self.log_dir=log_dir
    self.run_id=run_id
    self.zk=KazooClient(hosts=zk_connector)
    self.event=threading.Event()
    self.execution_attempt=0
    self.zk_connector=zk_connector
    self.fe_address=fe_address
 
  def reset_variables(self):
    self.attempt=0
    self.execution_failed=False
    self.event.clear()

  def start_periodic_check_timer(self):
    self.periodic_check=threading.Timer(10,self.check)
    self.periodic_check.start()

  def zk_query(self):
    if (self.zk.exists('/experiment/test/barriers')):
      children=self.zk.get_children('/experiment/test/barriers')
    else:
      children=None

    if self.zk.exists('/experiment/test/finished'):
      print('Test Execution for run_id:%d was successful'%(self.run_id))
      self.event.set()
      return

    if ((children is not None) and (len(children)>0) and (self.attempt>120)):
      print('Test Execution for run_id:%d has failed'%(self.run_id))
      self.execution_failed=True
      self.event.set()
      return
    
  def check(self):
    self.attempt+=1
    print('Attempt number:%d'%(self.attempt))
    if (self.attempt>120):
      self.execution_failed=True
      self.event.set()
      return
    try:
      self.zk.retry(self.zk_query)
    except (KazooException,RetryFailedError) as e:
      print('Caught KazooException')

    #install timer again
    self.start_periodic_check_timer()

  def run(self):
    self.execution_attempt+=1
    #start zk 
    self.zk.start()
    #delete finished znode if it exists
    if self.zk.exists('/experiment/test/finished'):
      self.zk.delete('/experiment/test/finished')

    #reset variables
    self.reset_variables()
    #start timer
    self.start_periodic_check_timer()
    #start test
    print('Starting test for run_id:%d and config:%s\n'%(self.run_id,self.config))
    args=['python','src/tests/rate_processing_interval_combinations4.py',\
      '-config',json.dumps(self.config),\
      '-log_dir',self.log_dir,\
      '-run_id','%d'%(self.run_id),\
      '-zk_connector',self.zk_connector,\
      '-fe_address',self.fe_address]
    p= subprocess.Popen(args)

    #wait for test to finish
    self.event.wait()

    #check execution status
    if self.execution_failed:
      print('Execution had failed.Exiting')
      #kill the experiment run
      p.kill()    
      #cancel periodic check timer
      self.periodic_check.cancel()
      #delete barriers 
      try:
        self.zk.delete('/experiment/test/barriers',recursive=True)
      except KazooException as e: 
        print('Caught KazooException')

      self.zk.stop()
      #kill existing endpoints 
      sub_placement=rate_processing_interval_combinations4.place('sub',\
        ['%s'%(tdesc.split(',')[4]) for tdesc in self.config])
      pub_placement=rate_processing_interval_combinations4.place('pub',\
        ['%s'%(tdesc.split(',')[3]) for tdesc in self.config])
      endpoint_machines= list(sub_placement.keys())+ list(pub_placement.keys())
      
      command_string='cd %s && ansible-playbook playbooks/util/kill.yml \
        --extra-vars="pattern=edgent.endpoints" --limit %s'%\
        (metadata.ansible,','.join(endpoint_machines))
      subprocess.check_call(['bash','-c', command_string])
      #check if endpoints are still running. If so, restart those nodes
      #wait for sometime 
      #restart test
      if (self.execution_attempt< 3):
        self.run()
    else:
      print('Execution was successful.Exiting')
      self.periodic_check.cancel()
      self.zk.stop()

if __name__=="__main__":
  zk_connector=metadata.public_zk4
  fe_address='10.20.30.29'
  runs=1

  k_colocation=5
  iterations=1120
  root_log_dir='/home/kharesp/log'

  for runid in range(1,runs+1,1):
    start_iter=601
    config_path='%s/configurations3/topics_%d'%(root_log_dir,k_colocation)
    log_dir= '%s/colocated_with_proc_%d_final/run%d'%\
      (root_log_dir,k_colocation,runid)

    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

    for iteration in range(start_iter,iterations+1,1):
      config=rate_processing_interval_combinations4.load_configuration('%s/%d'%\
        (config_path,iteration))

      print('Starting test runid:%d iteration:%d with config:%s\n'%(runid,iteration,config))
      h=Hawk(config,log_dir,iteration,zk_connector,fe_address)
      h.run()
