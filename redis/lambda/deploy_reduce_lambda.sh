#!/bin/bash -e

rm reduce_lambda.zip

zip -r reduce_lambda.zip reduce_lambda.py ../rediscluster ../redis_py_cluster-1.3.4.dist-info ../redis ../redis-2.10.6.dist-info
sh create_reduce_lambda.sh


