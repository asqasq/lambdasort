#!/bin/bash -e

#rm reduce_lambda.zip

zip -r reduce_lambda.zip reduce.py redis rediscluster
sh create_reduce_lambda.sh


