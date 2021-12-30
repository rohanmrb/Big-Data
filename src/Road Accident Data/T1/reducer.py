#!/usr/bin/env python3

import sys

di={}

for line in sys.stdin:
	line = line.strip()
	hour, count = line.split(',', 1)

	try:
		count = int(count)
		hour = int(hour)
	except ValueError:
		continue
	if hour in di :
		di[hour]+= 1
	else:
		di[hour] = 1
for h,c in sorted(di.items()):
	print('%d %d' %(h,c))
