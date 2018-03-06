#!/bin/bash -e

aws lambda create-function \
    --function-name reduce_lambda_network_crail \
    --region us-west-2 \
    --zip-file fileb://reduce_lambda.zip \
    --role arn:aws:iam::562930434285:role/pywren_exec_role_1 \
    --handler reduce_lambda_network_crail.lambda_handler \
    --runtime python2.7 \
    --timeout 300 \
    --memory-size 3008 \
    --vpc-config SubnetIds=subnet-b182b0ea,SecurityGroupIds=sg-755b0508



