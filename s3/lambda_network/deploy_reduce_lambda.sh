#!/bin/bash -e

rm reduce_lambda.zip

zip -r reduce_lambda.zip reduce_lambda_network_s3.py crail.py bin conf jars redis rediscluster ifcfg
sh create_reduce_lambda.sh


