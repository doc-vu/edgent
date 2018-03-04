import argparse,os

def process(log_dir):
  print(log_dir)
  hostname_mapping={
    'isislab30':'node17',
    'isislab25':'node18',
    'isislab24':'node19',
    'isislab23':'node20',
    'isislab22':'node21',
    'isislab21':'node22',
    'isislab26':'node23',
    'isislab27':'node24',
    'isislab28':'node25',
    'isislab29':'node26',
    'isislab19':'node27',
    'isislab17':'node28',
    'isislab15':'node29',
    'isislab14':'node30',
    'isislab13':'node31',
    'isislab12':'node32',
    'isislab11':'node33',
    'isislab10':'node34',
    'isislab9':'node35',
    'isislab8':'node36',
    'isislab7':'node37',
    'isislab6':'node38',
    'isislab5':'node39',
    'isislab2':'node40',
    'isislab1':'node41',
}
  for f in os.listdir(log_dir):
    if os.path.isfile(os.path.join(log_dir,f)):
      for key,value in hostname_mapping.items():
        if key in f:
          os.rename('%s/%s'%(log_dir,f),'%s/%s'%(log_dir,f.replace(key,value)))
          break

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for renaming hostname in files')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()

  for sub_dir in args.sub_dirs:
    #process files in log_dir/i
    process('%s/%s'%(args.log_dir,sub_dir))
