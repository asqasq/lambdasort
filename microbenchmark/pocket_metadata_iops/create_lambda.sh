#!/bin/bash -e

aws lambda create-function \
    --function-name pocket_metadata_iops \
    --region us-west-2 \
    --zip-file fileb://deploy.zip \
    --role arn:aws:iam::562930434285:role/pywren_exec_role_1 \
    --handler metadata_iops.lambda_handler \
    --runtime python2.7 \
    --timeout 120 \
    --memory-size 3008 \
    --vpc-config SubnetIds=subnet-7b8eea21,SecurityGroupIds=sg-6a85c114



