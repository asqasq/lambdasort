## Throughput Microbenchmark for Pocket
1. Create a Elasticache Redis Cluster (i.e. m4.large instance with 1 shard and 0 replicas) in the same VPC as the lambdas for checking lambda completions
2. Modify `create_lambda.sh` with the right IAM role and VPC config
2. Modify `throughput.py` with the right namenode_ip and Redis cluster host name 
3. Deploy the function with `./deploy_lambda.sh`
4. Run the throughput microbenchmark with 
```
./invoke_boto_async.py [number of lambdas] [request type: write/read/lookup/writebuffer/readbuffer] [number of iterations] [request size in bytes]
```
5. Check if all lambdas have completed with 
```
./check_redis.py [number of lambdas]
```
6. To re-run the latency microbenchamrk, restart Pocket namenode & datanode (since Pocket doesn't support overwriting of the same file) 
