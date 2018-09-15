import boto3
import os
import time
import pickle
import sys

from rediscluster import StrictRedisCluster

def wait_log(redis_client, n, log_name):
    num_keys = 0
    old_num_keys = 0
    while num_keys < n:
        keys = redis_client.keys(pattern="*"+log_name+"*")
        num_keys = len(keys)
        time.sleep(1)
        if num_keys != old_num_keys:
            print str(num_keys) + " lambdas out of " + str(n) + " finished"
            old_num_keys = num_keys

if __name__ == '__main__':
    n = int(sys.argv[1]) 

    # connect to redis
    redis_host = "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com"
    startup_nodes = [{"host": redis_host, "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
	
    wait_log(redis_client, n, "logs")
    print str(n) + " lambdas all finished"
    
    redis_client.flushall()
    print ">>> redis cluster flushed"
 
