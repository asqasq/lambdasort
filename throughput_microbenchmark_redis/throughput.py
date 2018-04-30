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

    def get_net_bytes(rxbytes, txbytes, rxbytes_per_s, txbytes_per_s, cpu_util):
        SAMPLE_INTERVAL = 1.0
        # schedule the function to execute every SAMPLE_INTERVAL seconds
        if STOP.is_set():
            threading.Timer(SAMPLE_INTERVAL, get_net_bytes, [rxbytes, txbytes, rxbytes_per_s, txbytes_per_s, cpu_util]).start() 
            rxbytes.append(int(ifcfg.default_interface()['rxbytes']))
            txbytes.append(int(ifcfg.default_interface()['txbytes']))
            rxbytes_per_s.append((rxbytes[-1] - rxbytes[-2])/SAMPLE_INTERVAL)
            txbytes_per_s.append((txbytes[-1] - txbytes[-2])/SAMPLE_INTERVAL) 
            util = psutil.cpu_percent(interval=1.0)
            cpu_util.append(util)

    # start collecting network data
    iface = ifcfg.default_interface()
    rxbytes = [int(iface['rxbytes'])]
    txbytes = [int(iface['txbytes'])]
    rxbytes_per_s = []
    txbytes_per_s = []
    cpu_util = []
    STOP.set()
    timelogger = TimeLog(enabled=True)
    get_net_bytes(rxbytes, txbytes, rxbytes_per_s, txbytes_per_s, cpu_util) 



    # create a file of size (datasize) bytes
    type = event['type']
    iter = int(event['iter'])
    datasize = int(event['datasize']) #bytes
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*datasize 
        f.write(text)

    #'''
    # connect to redis
    startup_nodes = [{"host": "rediscluster.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    if type == 'write':
	redis_write(redis_client, iter, text)
    elif type == 'read': 
        redis_read(redis_client, iter)
    else:
	return "Illegal type" 
    #'''
    
    '''
    #connect to s3
    s3_client = boto3.client('s3')
    if type == 'write':
	s3_write(s3_client, iter, text)
    elif type == 'read': 
        s3_read(s3_client, iter)
    else:
	return "Illegal type"
    '''
 
    # upload network data
    timelogger = TimeLog(enabled=True)
    startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    rclient = redis_client
    STOP.clear()
    upload_net_bytes(rclient, rxbytes_per_s, txbytes_per_s, cpu_util, timelogger, str(id))
    
    os.remove(file_tmp)

    return 


