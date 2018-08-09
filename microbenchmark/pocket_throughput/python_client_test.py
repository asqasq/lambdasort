import os, sys
import time
import pocket
import subprocess

from rediscluster import StrictRedisCluster
import pickle
import threading
import json

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
    	print "write ", dst_filename
        r = pocket.put(p, src_filename, dst_filename, jobid)
        if r != 0:
	    print "fail"
            raise Exception("put failed: "+ dst_filename)
    #return req_rate
    print "finished writing"
    
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
    print "lambda_handler..."
    id = int(event['id'])
    n = num_workers = int(event['n'])    
    type = event['type']
    iter = int(event['iter'])
    datasize = int(event['datasize']) #bytes
    #namenode_ip = "10.1.22.136"
    namenode_ip = "10.1.0.10"
    
    # create a file of size (datasize) bytes
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*datasize 
        f.write(text)

    print "connect..."
    # connect to pocket
    p = pocket.connect(namenode_ip, 9070)
    jobid = ""
    print "connected"

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
    #redis_host = "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com"
    redis_host = "rediscluster-log.e4lofi.clustercfg.usw2.cache.amazonaws.com"
    startup_nodes = [{"host": redis_host, "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    log = {'t':t2-t1, 't_start':t1}    
    log_str = pickle.dumps(log)
    key = '/throughput-log'+'-'+str(n)+'-'+str(id)
    redis_client.set(key, log_str)
    print key + " logged" 

    try:
        os.remove(file_tmp)
    except OSError:
        pass
    return type+" finished"


def main():
    n = int(sys.argv[1])
    type = sys.argv[2]
    iter = sys.argv[3]
    datasize = sys.argv[4]
    
    threads = []
    for i in range(n):
        d = {'id':str(i), 'n':str(n), 'iter':str(iter), 'datasize':str(datasize), 'type':type}
        #d = json.dumps(d)
	t = threading.Thread(target=lambda_handler, args=(d, None,))
	threads.append(t)
	t.start()

if __name__ == "__main__":
   main()
        
    
