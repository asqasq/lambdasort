#!/bin/bash -e

#rm reduce_lambda.zip

zip -r reduce_lambda.zip reduce.py redis rediscluster pocket.py libstdc++.so.6 libpocket.so libcppcrail.so libboost_python-py27.so.1.58.0
sh create_reduce_lambda.sh


