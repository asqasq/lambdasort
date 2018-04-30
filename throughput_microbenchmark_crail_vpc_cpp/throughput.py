import os
import time
import pocket

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil

def pocket_write(p, jobid, iter, src_filename):
    for i in xrange(iter):
        dst_filename = '/tmp'+'-'+str(i)
        r = pocket.put(p, src_filename, dst_filename, jobid)
        #if r != 0:
        #    raise Exception("put failed: "+ dst_filename)

def pocket_read(p, jobid, iter, src_filename):
    for i in xrange(iter):
        dst_filename = '/tmp'+'-'+str(i)
        r = pocket.get(p, dst_filename, src_filename, jobid)
        #if r != 0:
        #    raise Exception("get failed: "+ dst_filename)
        
def pocket_lookup(p, jobid, iter):
    for i in xrange(iter):
        dst_filename = '/tmp'+'-'+str(i)
        r = pocket.lookup(p, dst_filename, jobid)
        #if r != 0:
        #    raise Exception("lookup failed: "+ dst_filename)

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

    # connect to pocket
    p = pocket.connect("10.1.129.91", 9070)
    jobid = 'lambda3'
    r = pocket.register_job(p, jobid) # works if return 0
    if r != 0:
        print "registration failed"
        return

    if type == "write":
        pocket_write(p, jobid, iter, file_tmp)
    elif type == "read":
        pocket_read(p, jobid, iter, file_tmp)
    elif type == "lookup":
        pocket_lookup(p, jobid, iter)
    else:
        return "Illegal type"


    # upload network data
    timelogger = TimeLog(enabled=True)
    startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    rclient = redis_client
    STOP.clear()
    upload_net_bytes(rclient, rxbytes_per_s, txbytes_per_s, cpu_util, timelogger, str(id))
    
    os.remove(file_tmp)
    return

