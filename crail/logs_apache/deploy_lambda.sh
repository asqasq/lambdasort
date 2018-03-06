#!/bin/sh

rm test.zip
zip -r test.zip bin conf jars lambda_java.py crail.py 

sh lambda_create.sh
