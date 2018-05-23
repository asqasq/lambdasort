#!/bin/bash

python invoke_boto_async.py 250 mapper $1

python redis_check.py 250 map

python invoke_boto_async.py 250 reducer $1

python redis_check.py 250 reduce 


