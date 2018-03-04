import os
log_dir='/home/kharesp/log/distribution3_run2'
publishers=[10,9,8,7,6,5,4,3,2,1]

with open('output.csv','w') as f:
  f.write('#pub,#sub,90th_percentile_latency(ms),average_latency(ms)\n')
  for publisher in publishers:
    print('processing publisher:%d'%publisher)
    sub_dirs=os.listdir('%s/p%d'%(log_dir,publisher))
    sub_iterations=sorted([int(i) for i in sub_dirs])
    for sub in sub_iterations:
      #extract avg and 90th percentile latency
      with open('%s/p%d/%d/summary/overall_performance.csv'%(log_dir,publisher,sub),'r') as l:
        #skip header
        l.next()
        contents=l.next().split(',')
        latency_avg=float(contents[0])
        latency_90th=float(contents[7])
        f.write('%d,%d,%f,%f\n'%(publisher,sub,latency_90th,latency_avg))
