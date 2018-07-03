import os,argparse,sys

def filter(log_dir,count):
  if not os.path.exists('%s/original'%(log_dir)):
    os.makedirs('%s/original'%(log_dir))
  if not os.path.exists('%s/purged'%(log_dir)):
    os.makedirs('%s/purged'%(log_dir))

  files= os.listdir(log_dir)
  for f in files: 
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('t') ):
      os.rename('%s/%s'%(log_dir,f),'%s/original/%s'%(log_dir,f))

  files= os.listdir('%s/original'%(log_dir))
  for filename in files:
    with open('%s/original/%s'%(log_dir,filename),'r') as inp,\
      open('%s/purged/%s.csv'%(log_dir,filename.partition('_')[0]),'w') as out: 
      header= next(inp)
      out.write(header)
      for line in inp:
        reception_ts,container_id,sample_id,sender_ts,latency,eb_receive_ts,latency_to_eb,latency_from_eb= line.split(',') 
        if (int(sample_id)>=count): #sample_id starts from 0
          out.write(line)

if __name__=="__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for processing topic latency files')
  parser.add_argument('-count',type=int,help='# of initial samples to filter',required=True)
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()

  for sub_dir in args.sub_dirs:
    #filter out initial samples
    filter('%s/%s'%(args.log_dir,sub_dir),count)
