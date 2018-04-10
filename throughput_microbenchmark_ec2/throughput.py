import os
import time
import pickle
import crail

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil

def lambda_handler(event, context):
    # create a file of size (datasize)x1MB
    iter = 100
    datasize = 1 #MB
    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*1024*1024*datasize 
        f.write(text)

    # write to crail
    p = crail.launch_dispatcher("/home/ec2-user/lambdasort/throughput_microbenchmark_ec2/")
    socket = crail.connect()

    t0=time.time()
    ticket = 1001
    src_filename = file_tmp
    outer=1
    for j in xrange(outer):
        for i in xrange(iter):
            dst_filename = '/tmp...'+str(id)+'-'+str(i)
            #r = crail.put(socket, src_filename, dst_filename, ticket)
            r = crail.get(socket, dst_filename, src_filename, ticket)
            if r[-1] != u'\u0000':
                crail.close(socket, ticket, p)
                raise Exception("put failed: "+ dst_filename)
    t1=time.time()

    # print stats   
    throughput = outer*iter*datasize*8/(t1-t0)/1000
    print
    print "throughput (Gb/s) = " + str(throughput)
    print "time (s) = " + str(t1-t0)


    os.remove(file_tmp)
    crail.close(socket, ticket, p)

    return 


lambda_handler(0,0)
