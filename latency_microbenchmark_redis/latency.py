import boto3
import os
import time
import pickle

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil


def redis_write(rclient, iter, data):
    for i in xrange(iter):
        key = '/tmp'+'-'+str(i)
        rclient.set(key, data)
        #if result != True:
        #    return -1

def redis_read(rclient, iter):
    for i in xrange(iter):
        key = '/tmp'+'-'+str(i)
        rclient.get(key)
        #if result == None:
        #    return -1

def s3_write(s3_client, iter, data):
    bucket_name = "s3-microbenchmark"
    for i in range(iter):
        key = '/tmp'+'-'+str(i)
        result = s3_client.put_object(
            Bucket = bucket_name,
            Body = data,
            Key = key
        )

def s3_read(s3_client, iter):
    bucket_name = "s3-microbenchmark"
    for i in range(iter):
        key = '/tmp'+'-'+str(i)
        body = s3_client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
        #if body == None:
        #    return -1


def lambda_handler(event, context):
    id = int(event['id'])
    n = num_workers = int(event['n'])    

    # create a file of size (datasize) bytes
    iter = int(event['iter'])
    datasize = int(event['datasize']) #bytes
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*datasize 
        f.write(text)

    # connect to redis
    startup_nodes = [{"host": "rediscluster.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    
    t0=time.time()
    redis_write(redis_client, iter, text)
    t1=time.time()

    print "=========================================="
    print "Stats for "+str(iter)+" iter of "+str(datasize)+" bytes write:"
    print "time (s) = " + str(t1-t0)
    throughput = iter*datasize*8/(t1-t0)/1e9
    print "throughput (Gb/s) = " + str(throughput)
    print "latency (us) = " + str((t1-t0)/iter*1e6)
    print "=========================================="
    
    t0=time.time()
    redis_read(redis_client, iter)
    t1=time.time()

    print "=========================================="
    print "Stats for "+str(iter)+" iter of "+str(datasize)+" bytes read:"
    print "time (s) = " + str(t1-t0)
    throughput = iter*datasize*8/(t1-t0)/1e9
    print "throughput (Gb/s) = " + str(throughput)
    print "time (s) = " + str(t1-t0)
    print "latency (us) = " + str((t1-t0)/iter*1e6)
    print "=========================================="
 

    os.remove(file_tmp)

    return 



