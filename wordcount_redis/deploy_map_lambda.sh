#!/bin/bash -e

rm map_lambda.zip

zip -r map_lambda.zip map.py redis rediscluster test
sh create_map_lambda.sh


