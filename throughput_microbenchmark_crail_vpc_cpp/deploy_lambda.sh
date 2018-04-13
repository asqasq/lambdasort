#!/bin/bash -e

rm deploy.zip

zip -r deploy.zip throughput.py crail.py pocket.py libc.so.6 libstdc++.so.6 libpocket.so libcppcrail.so libboost_python-py27.so.1.58.0 bin conf jars redis rediscluster ifcfg psutil
sh create_lambda.sh


