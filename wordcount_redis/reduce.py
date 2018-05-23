import pickle, json
from rediscluster import StrictRedisCluster
import boto3 
import os 
import time

def lambda_handler(event, context):
    rid = int(event['rid'])
    n=num_mapper = int(event['num_mapper'])

    t0 = time.time()
    # read shuffle files 
    startup_nodes = [{"host": "rediscluster.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)

    word_count_list = []
    for i in xrange(num_mapper):
        shuffle_file = 'shuffle/shuffle-' + str(i) + '-' + str(rid)
        #with open(shuffle_file, 'r') as f:
        #    word_count_list += pickle.load(f)
        body = pickle.loads(redis_client.get(shuffle_file))
        word_count_list += body

    t1 = time.time()
    # add up word count 
    word_count = {}
    for (word,count) in word_count_list:
        if word in word_count:
            word_count[word] += count
        else:
            word_count[word] = count

    t2 = time.time()
    # write output to s3
    s3 = boto3.resource('s3')
    file_tmp = '/tmp/output'
    with open(file_tmp, "w+") as f:
        for k,v in word_count.items():
            f.write(str(k)+' '+str(v)+'\n')
    key = 'output/output' + str(rid)
    bucket_name = 'wordcount-yawen'
    s3.Bucket(bucket_name).upload_file(file_tmp, key)

    os.remove(file_tmp)

    t3 = time.time()
    
    
    # upload log
    startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    # t1-t0: intermediate read
    # t2-t1: adding word count
    # t3-t2: s3 write
    log = {'id': rid, 't0': t0, 't1': t1, 't2': t2, 't3': t3}
    key = 'reduce-log-'+str(n)+'-'+str(rid)
    redis_client.set(key, pickle.dumps(log))

    #print "reducer"+str(rid)+' finished'
    print key + ' logged'

