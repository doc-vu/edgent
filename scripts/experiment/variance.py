import numpy as np

runids=[1,2,6,4,5,6,7,8,9,10]
max_rate=25
interval=40

header=['rate']
for i in range(len(runids)):
  header.append('val%d'%(i+1))

header.append('mean')
header.append('std')

rates=range(1,max_rate+1)

if __name__=='__main__':
  with open('/home/kharesp/log/variance_%dms_90th.csv'%(interval),'w') as f: 
    f.write(','.join(header) + '\n')
    for rate in rates:
      f.write('%d,'%(rate))
      vals=[]
      for runid in runids:
        print('runid:%d rate:%d'%(runid,rate))
        with open('/home/kharesp/log/variance2/interval_%d/run%d/%d/summary/summary_topic.csv'%(interval,runid,rate),'r') as inp:
          next(inp) #skip header
          parts=next(inp).split(',') 
          f.write('%f,'%(float(parts[9])))
          vals.append(float(parts[9]))
      f.write('%f,%f\n'%(np.mean(vals),np.std(vals)))

  with open('/home/kharesp/log/variance_%dms_avg.csv'%(interval),'w') as f: 
    f.write(','.join(header) + '\n')
    for rate in rates:
      f.write('%d,'%(rate))
      vals=[]
      for runid in runids:
        with open('/home/kharesp/log/variance2/interval_%d/run%d/%d/summary/summary_topic.csv'%(interval,runid,rate),'r') as inp:
          next(inp) #skip header
          parts=next(inp).split(',') 
          f.write('%f,'%(float(parts[2])))
          vals.append(float(parts[2]))
      f.write('%f,%f\n'%(np.mean(vals),np.std(vals)))

  with open('/home/kharesp/log/variance_%dms_std.csv'%(interval),'w') as f: 
    f.write(','.join(header) + '\n')
    for rate in rates:
      f.write('%d,'%(rate))
      vals=[]
      for runid in runids:
        with open('/home/kharesp/log/variance2/interval_%d/run%d/%d/summary/summary_topic.csv'%(interval,runid,rate),'r') as inp:
          next(inp) #skip header
          parts=next(inp).split(',') 
          f.write('%s,'%(float(parts[15])))
          vals.append(float(parts[15]))
      f.write('%f,%f\n'%(np.mean(vals),np.std(vals)))
