from multiprocessing.pool import ThreadPool
import boto3
import json

def invoke_map_lambda(x, client):
    client = boto3.client('lambda')
    response = client.invoke(FunctionName="map_lambda",
                            #InvocationType='Event',
                            InvocationType='RequestResponse',
                            Payload=x)
    if response['StatusCode'] != 200:
        print "error"
    return 

def invoke_reduce_lambda(x, client):
    client = boto3.client('lambda')
    response = client.invoke(FunctionName="reduce_lambda",
                            #InvocationType='Event',
                            InvocationType='RequestResponse',
                            Payload=x)
    if response['StatusCode'] != 202:
        print "error"
    return 


n = 5
pool = ThreadPool(n)
client = boto3.client('lambda')


bucket_name = 'terasort-yawen'
path = '1TB/'

map_data_list = []
reduce_data_list = []
for i in range(n):
    map_d = {'id':str(i), 'n':str(n), 'bucket_name':bucket_name, 'path':path}
    map_d = json.dumps(map_d)
    map_data_list.append(map_d)
    reduce_d = {'id':str(i), 'n':str(n), 'bucket_name':bucket_name}
    reduce_d = json.dumps(reduce_d)
    reduce_data_list.append(reduce_d)


results = [pool.apply_async(invoke_map_lambda, args=(x, client)) for x in map_data_list]
#results = [pool.apply_async(invoke_reduce_lambda, args=(x, client)) for x in map_data_list]
output = [p.get() for p in results]

for i in output:
    a = 1

print 'finished'



