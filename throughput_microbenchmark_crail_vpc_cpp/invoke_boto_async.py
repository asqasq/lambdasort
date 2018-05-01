from multiprocessing.pool import ThreadPool
import boto3
import json
import botocore
import sys

def invoke_map_lambda(x, client):
    f = "lambda_crail_throughput_cpp"
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



n = int(sys.argv[1])
#iter = 200
#datasize = 1024*1024
iter = 5000 #20000
datasize = 1024

type = sys.argv[2]
#type = 'write'
max_p = 30
pool = ThreadPool(max_p)


map_data_list = []
for i in range(n):
    map_d = {'id':str(i), 'n':str(n), 'iter':str(iter), 'datasize':str(datasize), 'type':type}
    map_d = json.dumps(map_d)
    map_data_list.append(map_d)


client = 0
results = []

for i in range(n):
    r = pool.apply_async(invoke_map_lambda, args=(map_data_list[i], client))
    results.append(r)

#results = [pool.apply_async(invoke_map_lambda, args=(x, client)) for x in map_data_list]
#results = [pool.apply_async(invoke_reduce_lambda, args=(x, client)) for x in reduce_data_list]

output = [p.get() for p in results]

count = 0
for i in output:
    count += 1
    print count

print 'finished'



