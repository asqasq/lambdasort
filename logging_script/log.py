import time
import psutil
import ifcfg
import threading
import pickle

class TimeLog:
    def __init__(self, enabled=True):
	self.enabled = enabled
	self.start = time.time()
	self.prev = self.start
	self.points = []
	self.sizes = []

    def add_point(self, title):
	if not self.enabled:
	      return
	now = time.time()
	self.points += [(title, now - self.prev)]
	self.prev = now

def get_net_bytes(rxbytes, txbytes, rxbytes_per_s, txbytes_per_s, cpu_util):
    SAMPLE_INTERVAL = 1.0
    # schedule the function to execute every SAMPLE_INTERVAL seconds
    #if STOP.is_set():
	#threading.Timer(SAMPLE_INTERVAL, get_net_bytes, [rxbytes, txbytes, rxbytes_per_s, txbytes_per_s, cpu_util]).start()
    rxbytes.append(int(ifcfg.default_interface()['rxbytes']))
    txbytes.append(int(ifcfg.default_interface()['txbytes']))
    rxbytes_per_s.append((rxbytes[-1] - rxbytes[-2])/SAMPLE_INTERVAL)
    txbytes_per_s.append((txbytes[-1] - txbytes[-2])/SAMPLE_INTERVAL)
    util = psutil.cpu_percent(interval=1.0, percpu=True)
    cpu_util.append(util)


if __name__ == '__main__':
    # start collecting network data
    iface = ifcfg.default_interface()
    rxbytes = [int(iface['rxbytes'])]
    txbytes = [int(iface['txbytes'])]
    rxbytes_per_s = []
    txbytes_per_s = []
    cpu_util = []  
    timelogger = TimeLog(enabled=True)
 
    try:
	while(True):
	    get_net_bytes(rxbytes, txbytes, rxbytes_per_s, txbytes_per_s, cpu_util)
	    time.sleep(1)
    except KeyboardInterrupt:
	print "Saving logs..."
	log = {'started': timelogger.start,
             'rx': rxbytes_per_s,
             'tx': txbytes_per_s,
             'cpu': cpu_util}
	log_file = 'test.log'
	with open(log_file, 'w') as f:
	    pickle.dump(log, f)
