runs=range(1,6,1)
rates=range(1,11,1)

path='/home/kharesp/log/colocation_impact/topics_8'

rate_cpu={}

for rate in rates:
  rate_cpu[rate]=[]
  for run in runs:
    curr_dir='%s/run%d/%d'%(path,run,rate)
    with open('%s/features'%(curr_dir),'r') as f:
      next(f) #skip header
      parts=next(f).split(',')
      rate_cpu[rate].append(float(parts[6]))

for rate in rates:
  print(rate_cpu[rate])
