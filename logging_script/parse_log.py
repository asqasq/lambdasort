import pickle
import sys

log_file = sys.argv[1]
log = pickle.load(open(log_file))
print log


