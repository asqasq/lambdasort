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
    id = int(event['id'])
    n = num_workers = int(event['n'])
    bucket_name = str(event['bucket_name'])
    n_tasks = n

    t0=time.time()

    # connect to crail
    p = pocket.connect("10.1.0.10", 9070)
    #jobid = "" 
    jobid = str(event['id'])
    
    #read from input file: shuffle<0 id> shuffle<1 id> ... shuffle<id num_workers-1>
    #'''
    file_tmp = '/tmp/tmp'
    all_lines = []
    for i in xrange(n_tasks):
        key = 'shuffle' + str(i) +'-'+ str(id)
        src_filename = '/' + key
        dst_filename = file_tmp
        r = pocket.get(p, dst_filename, src_filename, jobid)
        if r != 0:
            raise Exception("get failed: "+ dst_filename)
            return  -1
        with open(file_tmp, "r") as f:
            all_lines+=f.readlines()
    os.remove(file_tmp)
    #'''
    
    t1 = time.time()

    t1_2 = time.time()

    #'''
    #merge & sort 
    for i in xrange(len(all_lines)):
        all_lines[i] = (all_lines[i][:10], all_lines[i][12:])
    all_lines.sort(key=lambda x: x[0])


    for i in xrange(len(all_lines)):
        all_lines[i] = all_lines[i][0]+"  "+all_lines[i][1]
    #'''
    t2=time.time()


    #[s3] write to output file: output<id>  
    s3 = boto3.resource('s3')
    file_name = 'output/sorted_output'
    m = 1000/n_tasks
    size = len(all_lines)/m
    for i in xrange(m):
        with open(file_tmp, "w+") as f:
            start = size*i
            end = start + size
            f.writelines(all_lines[start:end])
            f.seek(0)
            body = f.read()
        key = file_name + str(id*m+i)
        s3.Bucket(bucket_name).upload_file(file_tmp, key)

        os.remove(file_tmp)
    t3=time.time()


    # upload log
    startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    
    log = {'id': id, 't0': t0, 't1': t1_2, 't2': t2, 't3': t3}    
    log_str = pickle.dumps(log)
    key = '/reduce-log'+'-'+'100GB'+'-'+str(n)+'-'+str(id)
    redis_client.set(key, log_srt)

    #crail.close(socket, ticket, p)
    

    r = 'reduce finished ' + str(id)
    print r
    return r



