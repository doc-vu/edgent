import os
log_dir='/home/kharesp/log'
processing_intervals=[5,10,15,20,25,30,35,40,45,50]

for interval in processing_intervals:
  sub_dirs=os.listdir('%s/processing_%dms'%(log_dir,interval))
  sorted_dirs=sorted([int(d) for d in sub_dirs])
  for rate in sorted_dirs:
    files=os.listdir('%s/processing_%dms/%d'%(log_dir,interval,rate))
    cpu_files=[f for f in files if f.startswith('cpu')]
    pid_cpu_map={} 
    for filename in cpu_files:
      with open('%s/processing_%dms/%d/%s'%(log_dir,interval,rate,filename),'r') as f:
        #skip header
        next(f)
        ts= int(next(f).split(';')[2])
        pid_cpu_map[filename]=ts

    if(len(pid_cpu_map) > 1):
      filename= max(pid_cpu_map, key=pid_cpu_map.get)
      pid_cpu_map.pop(filename)

      for key in pid_cpu_map.keys():
        resource,node,pid= key.split('_')
        os.remove('%s/processing_%dms/%d/cpu_%s_%s'%(log_dir,interval,rate,node,pid)) 
        os.remove('%s/processing_%dms/%d/mem_%s_%s'%(log_dir,interval,rate,node,pid)) 
        os.remove('%s/processing_%dms/%d/nw_%s_%s'%(log_dir,interval,rate,node,pid)) 
        os.remove('%s/processing_%dms/%d/nw_filtered_%s_%s'%(log_dir,interval,rate,node,pid)) 
