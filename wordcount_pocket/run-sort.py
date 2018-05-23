import pocket 
from subprocess import Popen, PIPE
import time
import sys

# register job to pocket controller
jobname = sys.argv[1]
jobid = pocket.register_job(jobname, num_lambdas=0, capacityGB=12, peakMbps=23000) 
print "jobid: " + jobid + " registerd\n"

# start map stage
num_lambda = 100 #sort 10GB using 100 lambdas
cmd = "python invoke_boto_async.py " +str(num_lambda) + "  mapper " + jobid 
p = Popen(cmd.split(), stdout=PIPE)
result = p.communicate()[0]
print result + " \n"
# block until the map stage finishes
cmd = "python redis_check.py " +str(num_lambda) + "  map"
p = Popen(cmd.split(), stdout=PIPE)
result = p.communicate()[0]
print result + " \n"

# start reduce stage 
cmd = "python invoke_boto_async.py " +str(num_lambda) + "  reducer " + jobid 
p = Popen(cmd.split(), stdout=PIPE)
result = p.communicate()[0]
print result + " \n"
# block until the reduce stage finishes
cmd = "python redis_check.py " +str(num_lambda) + "  reduce"
p = Popen(cmd.split(), stdout=PIPE)
result = p.communicate()[0]
print result
print "job deregistered\n"

# deregister job with pocket controller & clear Redis 
print pocket.deregister_job(jobid)
cmd = "python flush_redis.py "
p = Popen(cmd.split(), stdout=PIPE)
result = p.communicate()[0]
print result + "\n"


