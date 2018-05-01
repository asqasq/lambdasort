#!/bin/bash -e

rm deploy.zip

zip -r deploy.zip latency.py pocket.py libc.so.6 libstdc++.so.6 libpocket.so libcppcrail.so libboost_python-py27.so.1.58.0 
sh create_lambda.sh


