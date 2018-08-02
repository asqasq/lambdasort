## Latency Microbenchmark for Pocket
1. Modify `create_lambda.sh` with the right IAM role and VPC config
2. Modify `latency.py` with the right namenode_ip
3. Deploy the function with `./deploy_lambda.sh`
4. In AWS lambda console, open function `pocket_latency`, then run it with the test button
5. To re-run the latency microbenchamrk, either change `jobid` in `latency.py` or restart Pocket namenode & datanode (since Pocket doesn't support overwriting of the same file) 
