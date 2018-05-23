import pickle, json
import re
import hashlib


mid = 0 
num_reducer = 100

# read words from input file 
input_file = 'test'
with open(input_file, 'r') as f:
    lines = f.readlines()
    #lines = [line.lower() for line in lines]

words = []
for line in lines:
    words += re.split(r'[^\w]+', line)
words = list(filter(None, words))

# count word frequency  
word_count = {} #(word, count) 
for word in words:
    if word in word_count:
        word_count[word] += 1
    else:
        word_count[word] = 1

# partition among reducers 
shuffle = {} #(rid, [(word,count), ...])
for k,v in word_count.items():
    rid = int(hashlib.md5(k).hexdigest(), 16) % num_reducer
    if rid in shuffle:
        shuffle[rid].append((k,v))
    else:
        shuffle[rid] = [(k,v)]


# write shuffle files
for k,v in shuffle.items():
    shuffle_file = 'shuffle/shuffle-'+str(mid)+'-'+str(k)
    with open(shuffle_file, 'w') as f:
        json.dump(v,f)
