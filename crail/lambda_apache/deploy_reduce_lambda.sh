#!/bin/bash -e

rm reduce_lambda.zip

zip -r reduce_lambda.zip reduce_lambda_crail.py crail.py bin conf jars 
sh create_reduce_lambda.sh


