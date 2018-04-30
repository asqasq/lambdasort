#!/bin/bash -e

rm deploy.zip

zip -r deploy.zip throughput.py redis rediscluster ifcfg psutil
sh create_lambda.sh


