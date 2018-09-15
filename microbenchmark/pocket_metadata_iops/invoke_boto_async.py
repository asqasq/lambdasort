from multiprocessing.pool import ThreadPool
import boto3
import json
import botocore
import sys
import pocket

def invoke_lambda(x, client):
    f = "pocket_metadata_iops"
    config = botocore.config.Config(connect_timeout=330, read_timeout=330)
    client = None
    while not client:
        try:
            client = boto3.client('lambda', config=config)
        except:
            print 'Exception'
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
type = sys.argv[2]
iter = sys.argv[3]
datasize = sys.argv[4]
#iter = 160
#datasize = 1024*1024
#iter = 5000 #20000
#datasize = 1024

max_p = 30
pool = ThreadPool(max_p)

data_list = []
for i in range(n):
    d = {'id':str(i), 'n':str(n), 'iter':str(iter), 'datasize':str(datasize), 'type':type}
    d = json.dumps(d)
    data_list.append(d)


client = 0
results = []

for i in range(n):
    r = pool.apply_async(invoke_lambda, args=(data_list[i], client))
    results.append(r)

output = [p.get() for p in results]

count = 0
for i in output:
    count += 1
    print count

print 'Invocation finished'

'''
namenode_ip = "10.1.22.136"
p = pocket.connect(namenode_ip, 9070)
jobid = ""
for i in xrange(n):
    filename = 'throughput-log'+'-'+str(n)+'-'+str(id)
    r = pocket.lookup(p, filename, jobid)
    while r:
	pocket.lookup(p, filename, jobid)
    print str(i) + "finished"
'''
