import pickle
import matplotlib.pyplot as plt
import sys
import numpy as np

log_file = sys.argv[1]
log = pickle.load(open(log_file))
#print log


log_cpu = np.array(log['cpu'])
num_cpu = log_cpu.shape[1]

fig = plt.figure()
for i in xrange(num_cpu):
    plt.subplot(num_cpu/2, 2, i+1) 
    plt.plot(log_cpu[:,i])
#plt.ylabel('')
plt.suptitle('CPU utilization (' + log_file + ')')
plt.show()
plt.savefig(log_file+'-cpu.png')

log_rx = np.array(log['rx'])*8.0/1e9 #Gb/s
fig = plt.figure()
plt.subplot(2,1,1)
plt.plot(log_rx)
plt.title('rx Gb/s')

log_tx = np.array(log['tx'])*8.0/1e9 #Gb/s
plt.subplot(2,1,2)
plt.plot(log_tx)
plt.title('tx Gb/s')

plt.suptitle('Throughput (' + log_file + ')')
plt.show()
plt.savefig(log_file+'-network.png')
