import boto3
import os
import time
import pickle
import crail

from rediscluster import StrictRedisCluster
import threading
import ifcfg
import psutil

import subprocess

def lambda_handler(event, context):
    result = []
    
    result = crail.launch_iobench()

    #p = subprocess.Popen(["./bin/crail", "iobench", "-t", "write", "-f", "/tmp.dat", "-s", "1048576", "-w", "0", "-k", "100"], stdout=subprocess.PIPE)
    #result.append(p.communicate()[0])
    
    for r in result:
        print r
   
    return 



