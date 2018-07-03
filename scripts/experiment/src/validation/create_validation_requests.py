import random

intervals= [10,20,30,40]

max_supported_rate={
  10: 78,
  20: 37,
  30: 24,
  40: 20,
}

def create_request(k):
  req=''
  for topic in range(1,k+1,1):
    #pick processing interval at random
    interval=intervals[random.randint(0,len(intervals)-1)]
    #pick publication rate at random
    rate=random.randint(1,max_supported_rate[interval]-1)
    #add to req string
    req+='t%d:%d:%d,'%(topic,interval,rate)
  return req.rstrip(',')

def create_validation_requests(k,n):
  requests=set()
  while (len(requests)<n):
    requests.add(create_request(k))
  return requests

def write_requests(requests,file_path):
  with open(file_path,'w') as f:
    for req in requests:
      f.write(req+'\n')

if __name__=="__main__":
  k=2
  n=50 
  file_path='/home/kharesp/workspace/java/edgent/model_learning/validation/configurations/%d_colocation'%(k)
  requests=create_validation_requests(k,n) 
  write_requests(requests,file_path)
