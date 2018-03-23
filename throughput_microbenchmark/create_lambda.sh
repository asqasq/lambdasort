#!/bin/bash -e

aws lambda create-function \
    --function-name lambda_crail_throughput \
    --region us-west-2 \
    --zip-file fileb://deploy.zip \
    --role arn:aws:iam::562930434285:role/pywren_exec_role_1 \
    --handler throughput.lambda_handler \
    --runtime python2.7 \
    --timeout 30 \
    --memory-size 3008 \
    --vpc-config SubnetIds=subnet-b182b0ea,SecurityGroupIds=sg-755b0508



