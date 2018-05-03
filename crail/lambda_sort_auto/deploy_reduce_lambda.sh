#!/bin/bash -e

rm reduce_lambda.zip

zip -r reduce_lambda.zip reduce_lambda_network_crail.py pocket.py libstdc++.so.6 libpocket.so libcppcrail.so libboost_python-py27.so.1.58.0 redis rediscluster ifcfg psutil
sh create_reduce_lambda.sh


