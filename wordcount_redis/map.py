import pickle, cPickle, json
import re
import hashlib
from rediscluster import StrictRedisCluster
import boto3
import os
import sys
import time

def lambda_handler(event, context):
    '''
    startup_nodes = [{"host": "rediscluster.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    client.flushall()
    print "Redis cluster flushed"
    return 
    '''
    
    
    mid = int(event['mid'])
    n=num_reducer = int(event['num_reducer']) #100

    t0 = time.time()
    #[s3] read from input file: input<id> 
    s3 = boto3.resource('s3')
    bucket_name = 'wordcount-yawen'
    #key = 'input/input' + str(mid)
    key = 'input/input_5MB'
    file_tmp = '/tmp/input'
    s3.Bucket(bucket_name).download_file(key, file_tmp)
    with open(file_tmp, "r") as f:
        lines = f.readlines() 

    t1 = time.time()

    words = []
    for line in lines:
        words += re.split(r'[^\w]+', line)
    words = list(filter(None, words))

    # count word frequency  
    word_count = {} #(word, count) 
    for word in words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

    # partition among reducers 
    shuffle = {} #(rid, [(word,count), ...])
    for k,v in word_count.items():
        rid = int(hashlib.md5(k).hexdigest(), 16) % num_reducer
        if rid in shuffle:
            shuffle[rid].append((k,v))
        else:
            shuffle[rid] = [(k,v)]

    t2 = time.time()

    # write shuffle files
    startup_nodes = [{"host": "rediscluster.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)

    for k,v in shuffle.items():
        shuffle_file = 'shuffle/shuffle-'+str(mid)+'-'+str(k)
        #with open(shuffle_file, 'w') as f:
        #    pickle.dump(v,f)
        value = pickle.dumps(v)
        redis_client.set(shuffle_file, value)

    t3 = time.time()

    # upload log
    startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    # t1-t0: s3 read
    # t2-t1: word counting & partition 
    # t3-t2: intermediate write
    log = {'id': mid, 't0': t0, 't1': t1, 't2': t2, 't3': t3}
    key = 'map-log-'+str(n)+'-'+str(mid)
    redis_client.set(key, pickle.dumps(log))

    os.remove(file_tmp)
    #print "mapper"+str(mid)+' finished'
    print key + ' logged'

