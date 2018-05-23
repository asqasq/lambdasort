#!/bin/bash -e

rm map_lambda.zip

zip -r map_lambda.zip map.py redis rediscluster test_small pocket.py libstdc++.so.6 libpocket.so libcppcrail.so libboost_python-py27.so.1.58.0 
sh create_map_lambda.sh


