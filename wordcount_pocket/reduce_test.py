import pickle, json

rid = 20
num_mapper = 1

# read shuffle files 
word_count_list = []
for i in xrange(num_mapper):
    shuffle_file = 'shuffle/shuffle-' + str(i) + '-' + str(rid)
    with open(shuffle_file, 'r') as f:
        word_count_list += json.load(f)

# add up word count 
word_count = {}
for (word,count) in word_count_list:
    if word in word_count:
        word_count[word] += count
    else:
        word_count[word] = count

with open("output/test", 'w') as f:
    for k,v in word_count.items():
        f.write(str(k)+' '+str(v)+'\n')

