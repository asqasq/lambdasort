from rediscluster import StrictRedisCluster

startup_nodes = [{"host": "rediscluster-log.a9ith3.clustercfg.usw2.cache.amazonaws.com", "port": "6379"}]
client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
client.flushall()
print "Redis cluster flushed"



