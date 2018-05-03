import os
log_dir='/home/kharesp/log/colocated_with_proc_iter1'
topics=[8,9,10,11,12,13,14,15,16,17,18,19,20]

with open('%s/sanity_check2.csv'%(log_dir),'w') as f:
  for count in topics:
    sub_dirs=os.listdir('%s/colocated_with_proc_%d'%(log_dir,count)) 
    sorted_dirs=sorted(sub_dirs)
    for iteration in sorted_dirs:
      with open('%s/colocated_with_proc_%d/%s/features'%(log_dir,count,iteration),'r') as inp:
        lines=inp.readlines()
        #discard header
        lines.pop(0)
        while(len(lines)>0):
          curr_line=lines.pop(0)
          curr_parts=curr_line.split(',')
          to_remove=[]
          toWrite=False
          for idx,line in enumerate(lines):
            parts=line.split(',')
            if ((curr_parts[2]==parts[2]) and (curr_parts[3]==parts[3])):
              print(curr_line)
              print(line)
              diff=float(curr_parts[6])-float(parts[6])
              if diff<0:
                diff*=-1
              print(diff)
              print('\n\n')
              to_remove.append(line)
              f.write('%f\n'%(diff))
          for line in to_remove:
            lines.remove(line)
