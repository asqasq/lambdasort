## Simple lambda microbenchmark for unloaded latency tests
import pywren
import time
import sys
import os
import crail
from subprocess import call, Popen
import json

def handler(id):
    p = crail.launch_dispatcher_from_lambda()

    call(["cp", "/var/task/lambda_java.py", "/tmp/lambda"]) 
    call(["cp", "/var/task/jars/crail-reflex-1.0.jar", "/tmp/crail-reflex"]) 
    #call(["cp", "/var/task/jars/crail-client-1.0.jar", "/tmp/crail-reflex"])

    socket = crail.connect()

    result = []
    result.append("Talk to dispatcher...")
    src_filename = "/tmp/crail-reflex"
    #dst_filename = "/dsttest-test-reflex2.data"
    dst_filename = "/data" + str(id)
    ticket = 1001
    result.append("Try PUT...")
    start = time.time()
    crail.put(socket, src_filename, dst_filename, ticket)
    '''
    for i in range(100):
        dst_filename = "/id" + str(id) + str(i)
        crail.put(socket, src_filename, dst_filename, ticket)
    '''
    end = time.time()
    result.append("Execution time for single PUT: " + str((end-start) * 1000000) + " us")

    crail.close(socket, ticket, p)
    return 0


    print "storing logs"
    result.append("storing logs")
    log = {'id': 1, 's3read': 2, 'compute': 3, 'write': 4}
    file_tmp = '/tmp/tmp'
    with open(file_tmp, "w") as f:
        f.write(json.dumps(log))
    src_filename = file_tmp
    dst_filename = '/map-logs-100GB'+str(n)
    crail.put(socket, src_filename, dst_filename, ticket)

    crail.close(socket, ticket, p)    
    return result

    time.sleep(1)
    #src_filename = "/dsttest-test-reflex2.data"
    src_filename = dst_filename
    dst_filename = "/tmp/crail-reflex-2"
    result.append("Now GET...")
    start = time.time()
    crail.get(socket, src_filename, dst_filename, ticket)
    end = time.time()
    result.append("Execution time for single GET: " + str((end-start) * 1000000) + " us")

    '''
    time.sleep(1)
    call(["ls", "-al", "/tmp/"])
    
    src_filename = "/dsttest-test-reflex2.data"
    print "Now DEL..."
    start = time.time()
    crail.delete(socket, src_filename, ticket)
    end = time.time()
    print "Execution time for single GET: ", (end-start) * 1000000, " us\n"
    '''

    return result

# single launch
#'''
wrenexec = pywren.default_executor()
future = wrenexec.call_async(handler, 100)
#for r in future.result():
#    print r
#'''

# parallel launch
'''
data_list = []
num_workers = 250
for i in range(num_workers):
    data_list.append(i)

#wrenexec = pywren.default_executor(job_max_runtime=120)
wrenexec = pywren.default_executor()
futures = wrenexec.map(handler, data_list)
results_map = pywren.get_all_results(futures)

print "finished"

#for r in results_map:
#    print r 
#    print ""
'''
