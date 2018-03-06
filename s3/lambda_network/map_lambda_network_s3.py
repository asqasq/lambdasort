import boto3
import os
import time
import pickle
#import crail

from rediscluster import StrictRedisCluster
import threading
import ifcfg

def lambda_handler(event, context):
    id = int(event['id'])
    n = num_workers = int(event['n'])
    bucket_name = str(event['bucket_name'])
    path = str(event['path'])
    n_tasks = n


    STOP = threading.Event()
    LOGS_PATH = 'map-logs-'+str(n)

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

    def upload_net_bytes(rclient, rxbytes_per_s, txbytes_per_s, timelogger, reqid):
        #rclient = redis.Redis(host=REDIS_HOSTADDR_PRIV, port=6379, db=0)  
        netstats = LOGS_PATH + '/netstats-' + reqid 
        rclient.set(netstats, str({'lambda': reqid,
             'started': timelogger.start,
             'rx': rxbytes_per_s,
             'tx': txbytes_per_s}).encode('utf-8'))
        print "wrote netstats"
        return

    def get_net_bytes(rxbytes, txbytes, rxbytes_per_s, txbytes_per_s):
        SAMPLE_INTERVAL = 1.0
        # schedule the function to execute every SAMPLE_INTERVAL seconds
        if STOP.is_set():
            threading.Timer(SAMPLE_INTERVAL, get_net_bytes, [rxbytes, txbytes, rxbytes_per_s, txbytes_per_s]).start() 
            rxbytes.append(int(ifcfg.default_interface()['rxbytes']))
            txbytes.append(int(ifcfg.default_interface()['txbytes']))
            rxbytes_per_s.append((rxbytes[-1] - rxbytes[-2])/SAMPLE_INTERVAL)
            txbytes_per_s.append((txbytes[-1] - txbytes[-2])/SAMPLE_INTERVAL) 


    t0=time.time()

    #[s3] read from input file: input<id> 
    s3 = boto3.resource('s3')
    file_local = '/tmp/input_tmp'
    lines = []
    # read 4 100MB files
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

    t1_2=time.time()
   
    # start collecting network data
    iface = ifcfg.default_interface()
    rxbytes = [int(iface['rxbytes'])]
    txbytes = [int(iface['txbytes'])]
    rxbytes_per_s = []
    txbytes_per_s = []
    STOP.set()
    get_net_bytes(rxbytes, txbytes, rxbytes_per_s, txbytes_per_s) 

    t2=time.time()

    file_tmp = file_local
    for i in xrange(n_tasks): #hardcode
        with open(file_tmp, "w+") as f:
            f.writelines(p_list[i])
            f.seek(0)
            p_list[i] = f.read()
        os.remove(file_tmp)


    #write to output files: shuffle<id 0> shuffle<id 1> shuffle<id num_workers-1>     
    s3_client = boto3.client('s3')
    for i in range(n_tasks):
        file_name = 'shuffle/'+'shuffle' + str(id) +'-'+ str(i)
        result = s3_client.put_object(
            Bucket = bucket_name,
            Body = p_list[i],
            Key = file_name
        )

    t3=time.time()


    #upload network data
    timelogger = TimeLog(enabled=True)
    startup_nodes = [{"host": "rediscluster.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
    redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    rclient = redis_client
    STOP.clear()
    upload_net_bytes(rclient, rxbytes_per_s, txbytes_per_s, timelogger, str(id))



    # upload log
    log = {'id': id, 't0': t0, 't1': t1, 't1_2': t1_2, 't2': t2, 't3': t3}
    key = 's3-map-logs-100GB-'+str(n)+'-'+str(id)
    redis_client.set(key, pickle.dumps(log))
    
    log = [t1-t0, t1_2-t1, t3-t2, t2-t1_2]
    key = 's3-map-results-100GB-'+str(n)+'-'+str(id)
    redis_client.set(key, pickle.dumps(log))    


    #return time spent (in sec) writing intermediate files 
    #return [t1-t0, t1_2-t1, t3-t2, t2-t1_2] #read input, compute, write shuffle 

    r = 'map finished ' + str(id)
    print r
    return r



