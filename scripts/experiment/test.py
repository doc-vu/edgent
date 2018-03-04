from __future__ import division 
import subprocess

samples=500
payload=4000
test_file='src/tests/publisher_stress_test.py'
log_dir='/home/kharesp/log'
duration=120*1000 #2 mins
rates=[1000,500,200,100,50,10,1]
connections=[1,2,10,20,50,100,200,500,1000]

total=0

info={}
for r in rates:
  sleep_intervals=[1000/(r/c) for c in connections]
  #d=[(c,s,duration/s) for s,c in zip(sleep_intervals,connections) if (s >=10)]
  d=[(c,s,samples) for s,c in zip(sleep_intervals,connections) if (s >=10)]
  info['r%d'%(r)]=[(p,s,c) for p,s,c in d if (((s*samples)/(1000*60))<=10)]
  print('rate:%d'%(r))
  print(info['r%d'%(r)])
  print('\n')

  ###run tests 
  for p,s,c in info['r%d'%(r)]:
    subprocess.check_call(['python',test_file,'test',str(int(p)),'1',str(int(p)),'1',str(int(s)),str(payload),str(int(c)),log_dir])
    subprocess.check_call(['python','src/plots/summarize/summarize.py','-log_dir',log_dir,'-sub_dirs',str(int(p))])

  #make directory 
  command_string='mkdir %s/r%d'%(log_dir,r)
  subprocess.check_call(['bash','-c',command_string])  

  #move test executions to above created directory
  command_string='cd %s && mv %s r%d'%(log_dir,' '.join(str(int(i)) for i in [p for p,s,c in info['r%d'%(r)]]),r)
  subprocess.check_call(['bash','-c',command_string])  
