import boto3
import os
import time
import pickle
import pocket

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil

def lambda_handler(event, context):
    '''
    p = pocket.connect("10.1.0.10", 9070)
    #print pocket.create_dir(p, 'sort', "")
    jobid = str(event['jobid']) #'sort'
    print pocket.put(p, 'pocket.py', 'tmp3', jobid)
    return
    '''
    
    id = int(event['id'])
    n = num_workers = int(event['n'])
    bucket_name = str(event['bucket_name'])
    path = str(event['path'])
    n_tasks = n


    log_file = []

    t0=time.time()

    #[s3] read from input file: input<id> 
    s3 = boto3.resource('s3')
    file_local = '/tmp/input_tmp'
    lines = []
    # read m 100MB files
    m = 1000/n_tasks
    for i in xrange(m):
        i += id*m
        key = path + 'input' + str(i)
        s3.Bucket(bucket_name).download_file(key, file_local)
        with open(file_local, "r") as f:
            lines += f.readlines() #each line contains a 100b record
        os.remove(file_local)

    t1=time.time()

    #partition 
    p_list = [[] for x in xrange(n_tasks)]  #list of n partitions  #hardcode
    for line in lines:
        key1 = ord(line[0])-32 # key range 32-126
        key2 = ord(line[1])-32
        #126-32+1=95
        #p = n/95 # 2500/(126-32+1) ~ 26.3 = 26
        #index = int(26.3*(key1+key2/95.0))  
        p = n_tasks/95.0 # total of 250 tasks 
        index = int(p*(key1+key2/95.0))
        p_list[index].append(line)

    t2=time.time()

    #write to output files: shuffle<id 0> shuffle<id 1> shuffle<id num_workers-1>     
    # connect to crail
    p = pocket.connect("10.1.12.156", 9070)
    #jobid = str(event['jobid']) #'sort'
    jobid = ""

    file_tmp = file_local
    for i in xrange(n_tasks):
        with open(file_tmp, "w") as f:
            f.writelines(p_list[i])
        key = 'shuffle' + str(id) +'-'+ str(i)
        src_filename = file_tmp
        dst_filename = '/' + key
        r = pocket.put(p, src_filename, dst_filename, jobid)
        if r != 0:
            raise Exception("put failed: "+ dst_filename)
            return -1
        #log_file.append((key, time.time()))
        
    t3=time.time()


    # upload log
    startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    
    log = {'id': id, 't0': t0, 't1': t1, 't2': t2, 't3': t3}
    log_str = pickle.dumps(log)
    key = '/map-log'+'-'+'100GB'+'-'+str(n)+'-'+str(id)
    redis_client.set(key, log_str)
    print key + " logged" 
    
    ''' 
    log_file_str = pickle.dumps(log_file)
    key = '/map-log-time'+'-'+'100GB'+'-'+str(n)+'-'+str(id)
    redis_client.set(key, log_file_str)
    print key + " logged" 
    '''
    os.remove(file_tmp)

    #crail.close(socket, ticket, p)


    r = 'map finished ' + str(id)
    print r
    return r





