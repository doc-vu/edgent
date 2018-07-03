from functools import reduce 
placement_file='/home/kharesp/static_placement/requests/skewed/n_50'
publisher_count=[]
with open(placement_file,'r') as f:
  for line in f:
    topic_descriptions=line.split(',')
    count= reduce(lambda x,y: x+y, [int(desc.split(':')[2]) for desc in topic_descriptions])
    publisher_count.append(count)

print(publisher_count)

print(max(publisher_count))
