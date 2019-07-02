import boto3
import os
import time
import pickle
import crail

def lambda_handler(event, context):
    id = int(event['id'])
    n = num_workers = int(event['n'])
    bucket_name = str(event['bucket_name'])
    path = str(event['path'])
    n_tasks = n

    t0=time.time()

    t1=time.time()

    t2=time.time()

    #write to output files: shuffle<id 0> shuffle<id 1> shuffle<id num_workers-1>     
    p = crail.launch_dispatcher_from_lambda()
    socket = crail.connect()

    ticket = 1001
    file_tmp = '/tmp/input_tmp'

    t3=time.time()

    # upload log
    log = {'id': id, 't0': t0, 't1': t1, 't2': t2, 't3': t3}
    file_tmp = '/tmp/tmp'
    with open(file_tmp, "w") as f:
        pickle.dump(log, f)
    src_filename = file_tmp
    dst_filename = '/invoke-logs-'+str(n)+'-'+str(id)
    r = crail.put(socket, src_filename, dst_filename, ticket)
    if r[-1] != u'\u0000':
        crail.close(socket, ticket, p)
        raise Exception("put failed: "+ dst_filename)

    os.remove(file_tmp)

    crail.close(socket, ticket, p)


    #return time spent (in sec) writing intermediate files 
    #return [t1-t0, t2-t1, t3-t2, t2-t2, [len(x)*100 for x in p_list]] #read input, compute, write shuffle 

    r = 'lambda finished ' + str(id)
    print r
    return r



