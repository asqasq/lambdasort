import os, sys
import time
import pocket
import subprocess

from rediscluster import StrictRedisCluster
import pickle


def pocket_write_buffer(p, jobid, iter, src, size, id):
    for i in xrange(iter):
        dst_filename = '/tmp'+str(id)+'-'+str(i)
        r = pocket.put_buffer(p, src, size, dst_filename, jobid)  
        if r != 0:
            raise Exception("put buffer failed: "+ dst_filename)
            
def pocket_read_buffer(p, jobid, iter, text_back, size, id):
    for i in xrange(iter):
        dst_filename = '/tmp'+str(id)+'-'+str(i)
        r = pocket.get_buffer(p, dst_filename, text_back, size, jobid)
        if r != 0:
            raise Exception("get buffer failed: "+ dst_filename)            


def pocket_write(p, jobid, iter, src_filename, id):
    for i in xrange(iter):
        dst_filename = '/tmp'+str(id)+'-'+str(i)
        r = pocket.put(p, src_filename, dst_filename, jobid)
        if r != 0:
            raise Exception("put failed: "+ dst_filename)
    #return req_rate
    
def pocket_read(p, jobid, iter, src_filename, id):
    for i in xrange(iter):
        dst_filename = '/tmp'+str(id)+'-'+str(i)
        r = pocket.get(p, dst_filename, src_filename, jobid)
        
        
def pocket_lookup(p, jobid, iter, id):
    req_rate = []
    t_start = time.time()
    num_req = 0
    for i in xrange(iter):
        dst_filename = '/tmp'+str(id)+'-'+str(i)
        r = pocket.lookup(p, dst_filename, jobid)
        #if r != 0:
        #    raise Exception("lookup failed: "+ dst_filename)
        num_req += 1
        t_now = time.time()
        if t_now - t_start >=1:
            req_rate.append(num_req)
            t_start = time.time()
            num_req = 0 
    return req_rate



def lambda_handler(event, context):
    id = int(event['id'])
    n = num_workers = int(event['n'])    
    type = event['type']
    iter = int(event['iter'])
    datasize = int(event['datasize']) #bytes
    namenode_ip = "10.1.22.136"
    
    # create a file of size (datasize) bytes
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*datasize 
        f.write(text)

    # connect to pocket
    p = pocket.connect(namenode_ip, 9070)
    jobid = ""

    if type == "write":
        t1=time.time()
        pocket_write(p, jobid, iter, file_tmp, id)
        t2=time.time()
    elif type == "read":
        t1=time.time()
        pocket_read(p, jobid, iter, file_tmp, id)
        t2=time.time()
    elif type == "lookup":
        t1=time.time()
        pocket_lookup(p, jobid, iter, id)
        t2=time.time()
    elif type == "writebuffer":
        t1=time.time()
        pocket_write_buffer(p, jobid, iter, text, datasize, id)
        t2=time.time()
    elif type == "readbuffer":
        text_back = " "*datasize
        t1=time.time()
        pocket_read_buffer(p, jobid, iter, text_back, datasize, id)
        t2=time.time()
    else:
        return "Illegal type"

     
    # upload network data
    redis_host = "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com"
    startup_nodes = [{"host": redis_host, "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    log = {'t':t2-t1, 't_start':t1}    
    log_str = pickle.dumps(log)
    key = '/throughput-log'+'-'+str(n)+'-'+str(id)
    redis_client.set(key, log_str)
    print key + " logged" 

    os.remove(file_tmp)
    return type+" finished"



