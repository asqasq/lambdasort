#!/bin/bash -e

rm deploy.zip

zip -r deploy.zip latency.py redis rediscluster ifcfg psutil
sh create_lambda.sh


