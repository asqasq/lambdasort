import boto3
import os
import time
import pickle
import sys

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil


def map_avg(redis_client, n):
  finished_workers=0
  n = n # number of workers
  logs_list = []

  for i in range(n):
    key = '/map-log'+'-'+'100GB'+'-'+str(n)+'-'+str(i)
    log_str = redis_client.get(key)
    log = pickle.loads(log_str)
    if log == None:
      #continue
      crail.close(socket, ticket, p)
      raise Exception("get failed: "+ key)
    else:
      finished_workers += 1
      logs_list.append(log)

    t_io = []
    t_comp = []
    t_inter = []
    t_prepare = []
    t_total = []
    #log = [t1-t0, t2-t1, t3-t2, t2-t2]
    for r in logs_list:
        t_io.append(r['t1']-r['t0'])
        t_comp.append(r['t2']-r['t1'])
        t_inter.append(r['t3']-r['t2'])
        t_prepare.append(r['t2']-r['t2'])
        t_total.append(r['t0']+r['t1']+r['t2']+r['t3'])
    print "map:" + str(n)
    print "finished:" + str(finished_workers)
    print "read input: " + str(sum(t_io) / len(t_io)) + "  max: " + str(max(t_io))
    print "compute: " + str(sum(t_comp) / len(t_comp)) + "  max: " + str(max(t_comp))
    print "prepare: " + str(sum(t_prepare) / len(t_prepare))  + "  max: " + str(max(t_prepare))
    print "write inter: " + str(sum(t_inter) / len(t_inter))  + "  max: " + str(max(t_inter))
    print "map_total: " + str(sum(t_total) / len(t_total))  + "  max: " + str(max(t_total))
    return


def reduce_avg(redis_client, n):
  finished_workers=0
  n = n # number of workers
  logs_list = []

  for i in range(n):
    key = '/reduce-log'+'-'+'100GB'+'-'+str(n)+'-'+str(i)
    log_str = redis_client.get(key)
    log = pickle.loads(log_str)
    if log == None:
      #continue
      crail.close(socket, ticket, p)
      raise Exception("get failed: "+ key)
    else:
      finished_workers += 1
      logs_list.append(log)
    
    #log = [t1-t0, t2-t1_2, t3-t2, t1_2-t1]
    t_io = []
    t_comp = []
    t_inter = []
    t_prepare = []
    t_total = []
    for r in logs_list:
        t_prepare.append(r['t1']-r['t1'])
        t_io.append(r['t3']-r['t2'])
        t_comp.append(r['t2']-r['t1'])
        t_inter.append(r['t1']-r['t0'])
        t_total.append(r['t0']+r['t1']+r['t2']+r['t3'])
    print "reduce:" + str(n)
    print "read inter: " + str(sum(t_inter) / len(t_inter)) + "  max: " + str(max(t_inter))
    print "prepare: " + str(sum(t_prepare) / len(t_prepare))  + "  max: " + str(max(t_prepare))
    print "compute: " + str(sum(t_comp) / len(t_comp)) + "  max: " + str(max(t_comp))
    print "write output: " + str(sum(t_io) / len(t_io)) + "  max: " + str(max(t_io))
    print "reduce_total: " + str(sum(t_total) / len(t_total))  + "  max: " + str(max(t_total))
    return

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
	
    if type == "map":
    	wait_log(n, "map-log")
        print str(n) + " mappers all finished"
    if type == "reduce":
	wait_log(n, "reduce-log")
        print str(n) + " reducers all finished"

    #map_avg(redis_client, n)
    #reduce_avg(redis_client, n)




