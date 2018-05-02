#!/bin/bash -e

rm map_lambda.zip

zip -r map_lambda.zip map_lambda_network_crail.py pocket.py libstdc++.so.6 libpocket.so libcppcrail.so libboost_python-py27.so.1.58.0 redis rediscluster ifcfg psutil
sh create_map_lambda.sh


