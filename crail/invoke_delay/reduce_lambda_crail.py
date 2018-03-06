import boto3
import os
import time
import pickle
import crail

def lambda_handler(event, context):
    id = int(event['id'])
    n = num_workers = int(event['n'])
    bucket_name = str(event['bucket_name'])
    n_tasks = n

    t0=time.time()

    p = crail.launch_dispatcher_from_lambda()
    socket = crail.connect()
    ticket = 1001

    #read from input file: shuffle<0 id> shuffle<1 id> ... shuffle<id num_workers-1>
    #'''
    file_tmp = '/tmp/tmp'
    all_lines = []
    for i in xrange(n_tasks):
        key = 'shuffle' + str(i) +'-'+ str(id)
        src_filename = '/' + key
        dst_filename = file_tmp
        r = crail.get(socket, src_filename, dst_filename, ticket)
        if r[-1] != u'\u0000':
            crail.close(socket, ticket, p)
            raise Exception("get failed: "+ src_filename)
        with open(file_tmp, "r") as f:
            all_lines+=f.readlines()
    os.remove(file_tmp)
    #'''
    
    t1 = time.time()


    #'''
    #merge & sort 
    for i in xrange(len(all_lines)):
        all_lines[i] = (all_lines[i][:10], all_lines[i][12:])
    all_lines.sort(key=lambda x: x[0])


    for i in xrange(len(all_lines)):
        all_lines[i] = all_lines[i][0]+"  "+all_lines[i][1]
    #'''
    t2=time.time()


    #[s3] write to output file: output<id>  
    s3 = boto3.resource('s3')
    file_name = 'output/sorted_output'
    m = 1000/n_tasks
    size = len(all_lines)/m
    for i in xrange(m):
        with open(file_tmp, "w+") as f:
            start = size*i
            end = start + size
            f.writelines(all_lines[start:end])
            f.seek(0)
            body = f.read()
        key = file_name + str(id*m+i)
        s3.Bucket(bucket_name).upload_file(file_tmp, key)

        os.remove(file_tmp)
    t3=time.time()

    # upload log
    log = {'id': id, 't0': t0, 't1': t1, 't2': t2, 't3': t3}
    file_tmp = '/tmp/tmp'
    with open(file_tmp, "w") as f:
        pickle.dump(log, f)
    src_filename = file_tmp
    dst_filename = '/reduce-logs-100GB-'+str(n)+'-'+str(id)
    ## new file
    r = crail.put(socket, src_filename, dst_filename, ticket)
    if r[-1] != u'\u0000':
        crail.close(socket, ticket, p)
        raise Exception("put failed: "+ dst_filename)
 
    log = [t1-t0, t2-t1, t3-t2, t1-t1]
    file_tmp = '/tmp/tmp'
    with open(file_tmp, "w") as f:
        pickle.dump(log, f)
    src_filename = file_tmp
    dst_filename = '/reduce-results-100GB-'+str(n)+'-'+str(id)
    ## new file
    r = crail.put(socket, src_filename, dst_filename, ticket)
    if r[-1] != u'\u0000':
        crail.close(socket, ticket, p)
        raise Exception("put failed: "+ dst_filename)



    crail.close(socket, ticket, p)
    #return time (in sec) spent reading intermediate files
    #return [t1-t0, t2-t1, t3-t2, t1-t1] #read shuffle, compute, write output 

    r = 'reduce finished ' + str(id)
    print r
    return r


