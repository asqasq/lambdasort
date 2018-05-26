import boto3
import os
import time
import pickle
import sys

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil
import numpy as np
import json

def lifetime(redis_client, n):
  logs_list_map = []
  logs_list_reduce = []

  for i in range(n):
    key = '/map-log-time'+'-'+'100GB'+'-'+str(n)+'-'+str(i)
    log_str = redis_client.get(key)
    log = pickle.loads(log_str)
    if log == None:
      #continue
      raise Exception("get failed: "+ key)
    else:
      logs_list_map.append(log)

  for i in range(n):
    key = '/reduce-log-time'+'-'+'100GB'+'-'+str(n)+'-'+str(i)
    log_str = redis_client.get(key)
    log = pickle.loads(log_str)
    if log == None:
      #continue
      raise Exception("get failed: "+ key)
    else:
      logs_list_reduce.append(log)
  
  d = {}
  for log in logs_list_map:
    for (key, time) in log:
      d[key] = time
  
  lifetime = []
  for log in logs_list_reduce:
    for (key, time) in log:
      lifetime.append(time-d[key])
  
  lifetime.sort()
  with open ("time.txt", 'w') as f:
    for item in lifetime:
      f.write("%f\n" % item)

  return


def map_avg(redis_client, n, bucket, suffix):
  finished_workers=0
  n = n # number of workers
  logs_list = []
  
  for i in range(n):
    #key = 'map-log'+'-'+str(n)+'-'+str(i)
    key = '/map-log'+'-'+'100GB'+'-'+str(n)+'-'+str(i)
    log_str = redis_client.get(key)
    if log_str == None:
      continue
      #raise Exception("get failed: "+ key)
    else:
      finished_workers += 1
      log = pickle.loads(log_str)
      logs_list.append(log)

  t_io = []
  t_comp = []
  t_inter = []
  t_total = []
  # t1-t0: s3 read
  # t2-t1: word counting & partition 
  # t3-t2: intermediate write
  #log = {'id': id, 't0': t0, 't1': t1, 't2': t2, 't3': t3}
  for r in logs_list:
      t_io.append(r['t1']-r['t0'])
      t_comp.append(r['t2']-r['t1'])
      t_inter.append(r['t3']-r['t2'])
      t_total.append(r['t3']-r['t0'])
  print "map:" + str(n)
  print "finished:" + str(finished_workers)
  print "read input: " + str(sum(t_io) / len(t_io)) + "  max: " + str(max(t_io))
  print "compute: " + str(sum(t_comp) / len(t_comp)) + "  max: " + str(max(t_comp))
  print "write inter: " + str(sum(t_inter) / len(t_inter))  + "  max: " + str(max(t_inter))
  print "map_total: " + str(sum(t_total) / len(t_total))  + "  max: " + str(max(t_total))

  s3_client = boto3.client('s3')
  bucket_name = bucket
  k = 'log/map-'+str(n)+'-'+suffix
  result = s3_client.put_object(
    Bucket = bucket_name,
    Body = json.dumps(logs_list),
    Key = k
  )

  t_start = []
  for r in logs_list:
      t_start.append(int(r['t0']))
  t_start.sort()
  t_start = np.array(t_start)
  t_min = t_start[0]
  t_start = t_start - t_min
  print t_start

  print t_io

def reduce_avg(redis_client, n, bucket, suffix):
  finished_workers=0
  n = n # number of workers
  logs_list = []

  for i in range(n):
    #key = 'reduce-log'+'-'+str(n)+'-'+str(i)
    key = '/reduce-log'+'-'+'100GB'+'-'+str(n)+'-'+str(i)
    log_str = redis_client.get(key)
    if log_str == None:
      #continue
      raise Exception("get failed: "+ key)
    else:
      finished_workers += 1
      log = pickle.loads(log_str)
      logs_list.append(log)

    
  # t1-t0: intermediate read
  # t2-t1: adding word count
  # t3-t2: s3 write
  #log = {'id': id, 't0': t0, 't1': t1, 't2': t2, 't3': t3}
  t_io = []
  t_comp = []
  t_inter = []
  t_total = []
  for r in logs_list:
      t_io.append(r['t3']-r['t2'])
      t_comp.append(r['t2']-r['t1'])
      t_inter.append(r['t1']-r['t0'])
      t_total.append(r['t3']-r['t0'])
  print "reduce:" + str(n)
  print "finished:" + str(finished_workers)
  print "read inter: " + str(sum(t_inter) / len(t_inter)) + "  max: " + str(max(t_inter))
  print "compute: " + str(sum(t_comp) / len(t_comp)) + "  max: " + str(max(t_comp))
  print "write output: " + str(sum(t_io) / len(t_io)) + "  max: " + str(max(t_io))
  print "reduce_total: " + str(sum(t_total) / len(t_total))  + "  max: " + str(max(t_total))

  s3_client = boto3.client('s3')
  bucket_name = bucket
  k = 'log/reduce-'+str(n)+'-'+suffix
  result = s3_client.put_object(
    Bucket = bucket,
    Body = json.dumps(logs_list),
    Key = k
  )

  t_start = []
  for r in logs_list:
      t_start.append(int(r['t0']))
  t_start.sort()
  t_start = np.array(t_start)
  t_min = t_start[0]
  t_start = t_start - t_min
  print t_start



def wait_log(n, log_name):
    num_keys = 0
    old_num_keys = 0 
    while num_keys < n:
        keys = redis_client.keys(pattern="*"+log_name+"*")
        num_keys = len(keys)
 	time.sleep(1)
	if num_keys != old_num_keys: 
	    print str(num_keys) + " lambdas out of " + str(n) + " finished"
	    old_num_keys = num_keys
    return 


if __name__ == '__main__':
    n = int(sys.argv[1]) #num_workers = int(event['n'])    
    type = sys.argv[2]

    # connect to redis
    startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)

    bucket = 'terasort-yawen'
    suffix = sys.argv[3] #'pocket-reflex-16nodes'
    #suffix = 'redis-pickle-1'
    #map_avg(redis_client, n, bucket, suffix)
    #reduce_avg(redis_client, n, bucket, suffix)

    	
    if type == "map":
    	#wait_log(n, "map-log")
        #print str(n) + " mappers all finished"
        map_avg(redis_client, n, bucket, suffix)
    if type == "reduce":
	wait_log(n, "reduce-log")
        print str(n) + " reducers all finished"
	reduce_avg(redis_client, n, bucket, suffix)
   	
    #lifetime(redis_client,n)



