import boto3
import os
import time
import pickle
#import crail
import pocket

#from rediscluster import StrictRedisCluster
#import threading
#import ifcfg
#import psutil

import subprocess

def lambda_handler(event, context):
    '''
    #/usr/lib64/libstdc++.so.6: version `CXXABI_1.3.9' not found (required by /var/task/libpocket.so)
    result = []
    p = subprocess.Popen(["ls", "/usr/lib64/libstdc++.so.6"], stdout=subprocess.PIPE)
    result.append(p.communicate()[0])
    for r in result:
        print r
    return 
    '''
    
    
    # create a file of size (datasize)x1MB
    iter = 100
    datasize = 1024*1024 #MB
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*datasize 
        f.write(text)

    # write to crail
    p = pocket.connect("10.1.204.149", 9060)
    jobid = 'lambda_20'
    print pocket.register_job(p, jobid) # works if return 0
    
    #print pocket.put(p, 'crail.py', 'crail_lambda', 'lambda')
    
    t0=time.time()
    ticket = 1001
    src_filename = file_tmp
    for i in xrange(iter):
        '''
        file_tmp = '/tmp/file_tmp'
        with open(file_tmp, 'w') as f:
            f.write(text)
        src_filename = file_tmp
        '''
        dst_filename = '/tmp'+'-'+str(i)
        r = pocket.put(p, src_filename, dst_filename, jobid)
        #r = pocket.get(p, dst_filename, src_filename, jobid)
        if r != 0:
            raise Exception("put failed: "+ dst_filename)
        with open(file_tmp, "r") as f:
            all_lines=f.readlines()
    t1=time.time()

    # print stats
    '''
    STOP.clear()
    print cpu_util
    print "tx:"
    print rxbytes_per_s
    print "rx:"
    print txbytes_per_s
    '''
    throughput = iter*datasize*8/(t1-t0)/1e9
    print "throughput (Gb/s) = " + str(throughput)
    print "time (s) = " + str(t1-t0)
    
    
    return 




