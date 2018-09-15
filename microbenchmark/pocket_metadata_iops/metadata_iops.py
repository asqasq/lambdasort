import os, sys
import time
import pocket
import subprocess

from rediscluster import StrictRedisCluster
import pickle


        
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


    LOGS_PATH = 'logs-'+str(n)
    STOP = threading.Event()

    class TimeLog:
        def __init__(self, enabled=True):
            self.enabled = enabled
            self.start = time.time()
            self.prev = self.start
            self.points = []
            self.sizes = []

        def add_point(self, title):
            if not self.enabled:
                  return
            now = time.time()
            self.points += [(title, now - self.prev)]
            self.prev = now

    def upload_net_bytes(rclient, rxbytes_per_s, txbytes_per_s, cpu_util, timelogger, reqid):
        #rclient = redis.Redis(host=REDIS_HOSTADDR_PRIV, port=6379, db=0)  
        netstats = LOGS_PATH + '/netstats-' + reqid 
        rclient.set(netstats, str({'lambda': reqid,
             'started': timelogger.start,
             'rx': rxbytes_per_s,
             'tx': txbytes_per_s,
             'cpu': cpu_util}).encode('utf-8'))
        print "wrote netstats"
        return
        
    STOP.set()
    timelogger = TimeLog(enabled=True)


    # create a file of size (datasize) bytes
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*datasize 
        f.write(text)

    # connect to pocket
    p = pocket.connect(namenode_ip, 9070)
    jobid = ""

    t1=time.time()
    req_per_s = pocket_lookup(p, jobid, iter, id)
    t2=time.time()
     

    # upload network data
    redis_host = "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com"
    startup_nodes = [{"host": redis_host, "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    rclient = redis_client
    STOP.clear()
    place_holder = [1]*len(req_per_s)
    upload_net_bytes(rclient, req_per_s, place_holder, place_holder, timelogger, str(id))
    print "iops stats uploaded"


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



