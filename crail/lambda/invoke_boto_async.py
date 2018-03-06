from multiprocessing.pool import ThreadPool
import boto3
import json
import botocore

def invoke_map_lambda(x, client):
    f = "map_lambda_crail"
    config = botocore.config.Config(connect_timeout=330, read_timeout=330)
    client = None
    while not client:
        try:
            client = boto3.client('lambda', config=config)
        except:
            print 'Exception in map'
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
    f = "reduce_lambda_crail"
    config = botocore.config.Config(connect_timeout=330, read_timeout=330)
    client = None
    while not client:
        try:
            client = boto3.client('lambda', config=config)
        except:
            print 'Exception in reduce'
            client = None    
    client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0
    response = client.invoke(FunctionName=f,
                            InvocationType='Event',
                            #InvocationType='RequestResponse',
                            Payload=x)
    if response['StatusCode'] != 202: #202
        print "error"
    return 


n = 1 #250
max_p = 20
pool = ThreadPool(max_p)

#config_dict = {'connect_timeout': 330, 'read_timeout': 330}
#config = botocore.client.Config(**config_dict)
#config = botocore.config.Config(connect_timeout=330, read_timeout=330)
#client = boto3.client('lambda', config=config)
#client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0

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

client = 0
results = []

for i in range(n):
    r = pool.apply_async(invoke_map_lambda, args=(map_data_list[i], client))
    #r = pool.apply_async(invoke_reduce_lambda, args=(reduce_data_list[i], client))
    results.append(r)

#results = [pool.apply_async(invoke_map_lambda, args=(x, client)) for x in map_data_list]
#results = [pool.apply_async(invoke_reduce_lambda, args=(x, client)) for x in reduce_data_list]

output = [p.get() for p in results]

count = 0
for i in output:
    count += 1
    print count

print 'finished'



