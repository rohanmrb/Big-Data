#!/usr/bin/env python3
import sys 

for x in sys.stdin:
    src,dest = (x.strip()).split()
    try:
        src = int(src)
        dest = int(dest)
    except ValueError:
        continue
    print(f'{src},{dest}')   
