#!/bin/bash -e

rm deploy.zip

zip -r deploy.zip throughput.py pocket.py libstdc++.so.6 libpocket.so libcppcrail.so libboost_python-py27.so.1.58.0 redis rediscluster ifcfg psutil 
sh create_lambda.sh


