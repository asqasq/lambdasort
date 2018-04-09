#!/bin/bash -e

rm deploy.zip

zip -r deploy.zip iobench.py crail.py bin conf jars redis rediscluster ifcfg psutil
sh create_lambda.sh


