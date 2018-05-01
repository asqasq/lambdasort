import os
import time
import pocket

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
    # create a file of size (datasize) in bytes
    iter = 10000
    datasize = 32 #bytes

    file_tmp = '/tmp/file_tmp'
    with open(file_tmp, 'w') as f:
        text = 'a'*datasize
        f.write(text)

    # write to crail
    p = pocket.connect("10.1.129.91", 9070)
    jobid = 'lambda3'
    r = pocket.register_job(p, jobid) # works if return 0
    if r != 0:
        print "registration failed"
        return

    time_list = []
    t0=time.time()
    pocket_write(p, jobid, iter, file_tmp)
    t1=time.time()

    print "=========================================="
    #print np.percentile(time_list, 90)
    print "Stats for "+str(iter)+" iter of "+str(datasize)+" bytes write:"
    throughput = iter*datasize*8/(t1-t0)/1e9
    print "throughput (Gb/s) = " + str(throughput)
    print "time (s) = " + str(t1-t0)
    print "latency (us) = " + str((t1-t0)/iter*1e6)
    print "=========================================="


    t0=time.time()
    pocket_read(p, jobid, iter, file_tmp)
    t1=time.time()

    print "=========================================="
    print "Stats for "+str(iter)+" iter of "+str(datasize)+" bytes read:"
    throughput = iter*datasize*8/(t1-t0)/1e9
    print "throughput (Gb/s) = " + str(throughput)
    print "time (s) = " + str(t1-t0)
    print "latency (us) = " + str((t1-t0)/iter*1e6)
    print "=========================================="


    t0=time.time()
    pocket_lookup(p, jobid, iter)
    t1=time.time()

    print "=========================================="
    print "Stats for "+str(iter)+" iter of "+str(datasize)+" bytes lookup:"
    throughput = iter*datasize*8/(t1-t0)/1e9
    print "throughput (Gb/s) = " + str(throughput)
    print "time (s) = " + str(t1-t0)
    print "latency (us) = " + str((t1-t0)/iter*1e6)
    print "=========================================="

    os.remove(file_tmp)
    return

