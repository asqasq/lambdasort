from multiprocessing.pool import ThreadPool
import boto3
import json
import botocore
import sys

def invoke_map_lambda(x, client):
    f = "map_lambda_network_cpu_crail_apache_vpc_cpp"
    config = botocore.config.Config(connect_timeout=330, read_timeout=330)
    client = None
    while not client:
        try:
            client = boto3.client('lambda', config=config)
        except:
            #print 'Exception in map'
            client = None    
    client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0 
    response = client.invoke(FunctionName=f,
                            InvocationType='Event',
                            #InvocationType='RequestResponse',
                            Payload=x)
    if response['StatusCode'] != 202:
        print "error"
    return 

def invoke_reduce_lambda(x, client):
    f = "reduce_lambda_network_cpu_crail_apache_vpc_cpp"
    config = botocore.config.Config(connect_timeout=330, read_timeout=330)
    client = None
    while not client:
        try:
            client = boto3.client('lambda', config=config)
        except:
            #print 'Exception in reduce'
            client = None    
    client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0
    response = client.invoke(FunctionName=f,
                            InvocationType='Event',
                            #InvocationType='RequestResponse',
                            Payload=x)
    if response['StatusCode'] != 202: #202
        print "error"
    return 

#python invoke_boto_async.py [num workers] [worker type]
#i.e.
#   python invoke_boto_async.py 250 mapper

n = int(sys.argv[1])#500
worker = sys.argv[2]
max_p = 30
pool = ThreadPool(max_p)
jobid = sys.argv[3]

bucket_name = 'terasort-yawen'
path = '1TB/'

map_data_list = []
reduce_data_list = []
for i in range(n):
    map_d = {'jobid':str(jobid), 'id':str(i), 'n':str(n), 'bucket_name':bucket_name, 'path':path}
    map_d = json.dumps(map_d)
    map_data_list.append(map_d)
    reduce_d = {'jobid':str(jobid), 'id':str(i), 'n':str(n), 'bucket_name':bucket_name}
    reduce_d = json.dumps(reduce_d)
    reduce_data_list.append(reduce_d)

client = 0
results = []

if worker=='mapper':
    print 'start '+str(n)+' mappers'
if worker=='reducer':
    print 'start '+str(n)+' reducers'

for i in range(n):
    if worker=='mapper':
        r = pool.apply_async(invoke_map_lambda, args=(map_data_list[i], client))
        #print str(i)
    if worker=='reducer':
        r = pool.apply_async(invoke_reduce_lambda, args=(reduce_data_list[i], client))
        #print str(i)
    results.append(r)


output = [p.get() for p in results]

count = 0
for i in output:
    count += 1
    print count

print 'finished'



