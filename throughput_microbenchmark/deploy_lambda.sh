#!/bin/bash -e

rm deploy.zip

zip -r deploy.zip throughput.py crail.py bin conf jars redis rediscluster ifcfg psutil
sh create_lambda.sh


