#!/bin/bash -e

rm map_lambda.zip

zip -r map_lambda.zip map_lambda.py ../rediscluster ../redis_py_cluster-1.3.4.dist-info ../redis ../redis-2.10.6.dist-info
sh create_map_lambda.sh


