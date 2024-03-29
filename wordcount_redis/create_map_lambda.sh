#!/bin/bash -e

aws lambda create-function \
    --function-name map_wordcount_redis \
    --region us-west-2 \
    --zip-file fileb://map_lambda.zip \
    --role arn:aws:iam::562930434285:role/pywren_exec_role_1 \
    --handler map.lambda_handler \
    --runtime python2.7 \
    --timeout 300 \
    --memory-size 3008 \
    --vpc-config SubnetIds=subnet-2f940275,SecurityGroupIds=sg-aadd03d4



