#!/usr/bin/env python3
import sys

cur_src=None
lis = []
v_path = sys.argv[1]
fopen = open(v_path,'w+')
for line in sys.stdin:	
    line = line.strip()
    src,dest = line.split(',')
    try:
        src = int(src)
        dest =  int(dest)
    except ValueError:
        continue
    if cur_src is None :
        cur_src = src
        lis.append(dest)
    elif cur_src == src:
        lis.append(dest)
    else:
        print(f'{cur_src}${lis}')
        fopen.write(f'{cur_src},1\n')
        cur_src = src 
        lis  = [dest]
print(f'{cur_src}${lis}')
fopen.write(f'{cur_src},1')
fopen.close()
