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

    t2=time.time()

    #write to output files: shuffle<id 0> shuffle<id 1> shuffle<id num_workers-1>     
    p = crail.launch_dispatcher_from_lambda()
    socket = crail.connect()

    ticket = 1001
    file_tmp = file_local
    for i in xrange(n_tasks):
        with open(file_tmp, "w") as f:
            f.writelines(p_list[i])
        key = 'shuffle' + str(id) +'-'+ str(i)
        src_filename = file_tmp
        dst_filename = '/' + key
        r = crail.put(socket, src_filename, dst_filename, ticket)
        if r[-1] != u'\u0000':
            crail.close(socket, ticket, p)
            raise Exception("put failed: "+ dst_filename)

    t3=time.time()

    # upload log
    log = {'id': id, 't0': t0, 't1': t1, 't2': t2, 't3': t3, 'file_size': [len(x)*100 for x in p_list]}
    file_tmp = '/tmp/tmp'
    with open(file_tmp, "w") as f:
        pickle.dump(log, f)
    src_filename = file_tmp
    dst_filename = '/map-logs-100GB-'+str(n)+'-'+str(id)
    r = crail.put(socket, src_filename, dst_filename, ticket)
    if r[-1] != u'\u0000':
        crail.close(socket, ticket, p)
        raise Exception("put failed: "+ dst_filename)

    log = [t1-t0, t2-t1, t3-t2, t2-t2]
    file_tmp = '/tmp/tmp'
    with open(file_tmp, "w") as f:
        pickle.dump(log, f)
    src_filename = file_tmp
    dst_filename = '/map-results-100GB-'+str(n)+'-'+str(id)
    r = crail.put(socket, src_filename, dst_filename, ticket)
    if r[-1] != u'\u0000':
        crail.close(socket, ticket, p)
        raise Exception("put failed: "+ dst_filename)


    os.remove(file_tmp)

    crail.close(socket, ticket, p)


    #return time spent (in sec) writing intermediate files 
    #return [t1-t0, t2-t1, t3-t2, t2-t2, [len(x)*100 for x in p_list]] #read input, compute, write shuffle 

    r = 'map finished ' + str(id)
    print r
    return r



