#!/usr/bin/env python3

import sys

cur_dest=None
sum =0
rank =0
for line in sys.stdin:	
    line = line.strip()
    dest,_,contri = line.split(',')
    try:
        dest =  int(dest)
        contri = float(contri)
    except ValueError:
        continue
    if (cur_dest is None or cur_dest == dest):
        cur_dest = dest
        sum = sum + contri
    else:
        rank = 0.15 +0.85*sum
        print(f'{cur_dest},{rank:.2f}')
        sum = contri
        cur_dest = dest
rank = 0.15 +0.85*sum
print(f'{cur_dest},{rank:.2f}')



