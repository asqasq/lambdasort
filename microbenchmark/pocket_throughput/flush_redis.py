import boto3
import os
import time
import pickle
import sys

from rediscluster import StrictRedisCluster


if __name__ == '__main__':

    # connect to redis
    #redis_host = "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com"
    redis_host = "rediscluster-log.e4lofi.clustercfg.usw2.cache.amazonaws.com"
    startup_nodes = [{"host": redis_host, "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
	
    redis_client.flushall()
    print ">>> redis cluster flushed"
 
