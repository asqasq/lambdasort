import boto3
import os
import time
import pickle
import crail

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil

def lambda_handler(event, context):
    id = int(event['id'])
    n = num_workers = int(event['n'])
    

    STOP = threading.Event()
    LOGS_PATH = 'logs-'+str(n)

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



    # create a file of size (datasize)x1MB
    iter = 100
    datasize = 1 #MB
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*1024*1024*datasize 
        f.write(text)

    # write to crail
    p = crail.launch_dispatcher_from_lambda()
    socket = crail.connect()

    t0=time.time()
    ticket = 1001
    src_filename = file_tmp
    for i in xrange(iter):
        dst_filename = '/tmp'+str(id)+'-'+str(i)
        r = crail.put(socket, src_filename, dst_filename, ticket)
        #r = crail.get(socket, dst_filename, src_filename, ticket)
        if r[-1] != u'\u0000':
            crail.close(socket, ticket, p)
            raise Exception("put failed: "+ dst_filename)
    t1=time.time()


    # upload network data
    STOP.clear()
    print cpu_util
    print "tx:"
    print rxbytes_per_s
    print "rx:"
    print txbytes_per_s
    throughput = iter*datasize*8/(t1-t0)/1000
    print "throughput (Gb/s) = " + str(throughput)
    print "time (s) = " + str(t1-t0)
    
    '''
    timelogger = TimeLog(enabled=True)
    startup_nodes = [{"host": "rediscluster.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    rclient = redis_client
    STOP.clear()
    upload_net_bytes(rclient, rxbytes_per_s, txbytes_per_s, cpu_util, timelogger, str(id))
    '''
    
    
    # upload throughput data
    '''
    throughput = datasize/(t1-t0)
    log = {'throughput':throughput}
    file_tmp = '/tmp/tmp'
    with open(file_tmp, "w") as f:
        pickle.dump(log, f)
    src_filename = file_tmp
    dst_filename = '/throughput-logs-'+str(n)+'-'+str(id)
    ## new file
    r = crail.put(socket, src_filename, dst_filename, ticket)
    if r[-1] != u'\u0000':
        crail.close(socket, ticket, p)
        raise Exception("put failed: "+ dst_filename)
    '''


    os.remove(file_tmp)

    crail.close(socket, ticket, p)

    return 

