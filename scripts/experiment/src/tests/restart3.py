import threading,time,sys,os,signal,subprocess,json
from kazoo.client import KazooClient
from kazoo.retry import RetryFailedError
from kazoo.exceptions import KazooException
import rate_processing_interval_combinations3
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,send_mail,util
import zmq,json

class Hawk(object):
  def __init__(self,config,log_dir,run_id,\
    zk_connector,fe_address,mq_connector,liveliness_mins):
    self.config=config
    self.log_dir=log_dir
    self.run_id=run_id
    self.zk=KazooClient(hosts=zk_connector)
    self.event=threading.Event()
    self.execution_attempt=0
    self.zk_connector=zk_connector
    self.fe_address=fe_address

    self.mq_connector=mq_connector
    self.context=zmq.Context(1)
    self.sockets=[]
    for connector in mq_connector.split(','):
      socket=self.context.socket(zmq.PUSH)
      socket.connect(connector)
      self.sockets.append(socket)

    self.periodic_check_timer_interval_sec=10
    self.grace_period_mins=3
    self.execution_failure_attempts=((liveliness_mins+\
      self.grace_period_mins)*60)/self.periodic_check_timer_interval_sec

  def reset_variables(self):
    self.attempt=0
    self.execution_failed=False
    self.event.clear()

  def start_periodic_check_timer(self):
    self.periodic_check=threading.Timer(self.periodic_check_timer_interval_sec,
      self.check)
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

    if ((children is not None) and (len(children)>0) and (self.attempt>self.execution_failure_attempts)):
      print('Test Execution for run_id:%d has failed'%(self.run_id))
      self.execution_failed=True
      self.event.set()
      return
    
  def check(self):
    self.attempt+=1
    print('Attempt number:%d'%(self.attempt))
    if (self.attempt>self.execution_failure_attempts):
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
    args=['python','src/tests/rate_processing_interval_combinations3.py',\
      '-config',json.dumps(self.config),\
      '-log_dir',self.log_dir,\
      '-run_id','%d'%(self.run_id),\
      '-zk_connector',self.zk_connector,\
      '-fe_address',self.fe_address,\
      '-mq_connector',self.mq_connector]
    p= subprocess.Popen(args)

    #wait for test to finish
    self.event.wait()

    #check execution status
    if self.execution_failed:
      print('Execution Attempt:%d has failed.'%(self.execution_attempt))
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
      sub_placement=rate_processing_interval_combinations3.place('sub',\
        ['%s'%(tdesc.split(',')[4]) for tdesc in self.config])
      pub_placement=rate_processing_interval_combinations3.place('pub',\
        ['%s'%(tdesc.split(',')[3]) for tdesc in self.config])
      endpoint_machines=list(sub_placement.keys())+ list(pub_placement.keys())
      
      #command_string='cd %s && ansible-playbook playbooks/util/kill.yml \
      #  --extra-vars="pattern=edgent.endpoints" --limit %s'%\
      #  (metadata.ansible,','.join(endpoint_machines))
      #subprocess.check_call(['bash','-c', command_string])
      
      util.kill2(self.sockets,','.join(endpoint_machines))
      #check if endpoints are still running. If so, restart those nodes
      #wait for sometime 
      #restart test
      if (self.execution_attempt< 3):
        time.sleep(5)
        self.run()
      else:
        print('All execution attempts for test:%d failed.'%(self.run_id))
        self.cleanup()
    else:
      print('Execution attempt:%d was successful.Exiting'%(self.execution_attempt))
      self.periodic_check.cancel()
      self.cleanup()

  def cleanup(self):
    self.zk.stop()
    for socket in self.sockets:
      socket.setsockopt(zmq.LINGER,0)
      socket.close()
    self.context.destroy()


if __name__=="__main__":
  zk_connector=metadata.public_zk3
  fe_address='10.20.30.5' 
  runs=1

  k_colocation=3
  mq_connector='tcp://129.59.104.151:3025'
  root_log_dir='/home/kharesp/workspace/java/edgent/model_learning_urd/training'

  for runid in range(1,runs+1,1):
    config_file='%s/configurations/%d_colocation'%(root_log_dir,k_colocation)
    with open(config_file,'r') as f: 
      for idx,line in enumerate(f):
        #if (idx+1) in [186,747,827,297,481,525]:
        if (idx+1) in [687, 747, 748, 761, 810, 909, 924, 945, 965]:
          log_dir= '%s/test_interval_impact/%d_colocation/between_.04_and_.09/run%d/%d'%(root_log_dir,k_colocation,runid,idx+1)
          if not os.path.exists(log_dir):
            os.makedirs(log_dir)
          for l in [2,5,10,20,40]:
            if (idx+1==687) and (l<=20):
              continue
            config=rate_processing_interval_combinations3.create_configuration(line.rstrip(),liveliness=l*60)
            
            print('Starting test runid:%d iteration:%d liveliness:%d mins with config:%s\n\n\n'%(runid,idx+1,l,config))
            start_time=time.time()
            h=Hawk(config,log_dir,l,\
              zk_connector,fe_address,mq_connector,l)
            h.run()
            end_time=time.time()
          
            elapsed_time_min=(end_time-start_time)/60.0
            print('\n\nRunid:%d iteration:%d liveliness:%d mins took %f mins to finish\n\n'%(runid,idx+1,l,elapsed_time_min))
