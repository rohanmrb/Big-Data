#!/usr/bin/env python3

import sys, json 

def cosine_similarity(v1,v2):
    sumxx, sumxy, sumyy = 0, 0, 0
    for i in range(len(v1)):
        x = v1[i]; y = v2[i]
        sumxx += x*x
        sumyy += y*y
        sumxy += x*y
    return (sumxy/(sumxx*sumyy)**0.5)

def contribution(v1,v2,len):
    return ((v1*v2)/len)


filev = open(sys.argv[1],'r')
lines = filev.readlines()
filev.close()

dest = set()
fileE = open(sys.argv[2],'r')
emb = json.load(fileE)
fileE.close()

rank = {}
for line in lines:
    src,ran = (line.strip()).split(',')
    if not (src in rank.keys()):
        rank[src] = float(ran)

for x in sys.stdin:
    src,lis = x.split('$')
    lis = json.loads(lis)
    for i in lis:
        dest.add(str(i))
        similarity = cosine_similarity(emb[str(src).strip()],emb[str(i)])
        con = contribution(rank[str(src)],similarity,len(lis))
        print (f'{i},{src},{con}')


for i in rank.keys():
    if not (i in dest):
        print (f'{int(i)},0,0')
