#!/usr/bin/env python3

import sys

di = {}

for line in sys.stdin:
	state, city, count = line.split(',')

	if state in di :
	
		if city in di[state]:
			di[state][city] += 1
		else:
			di[state][city] = 1
	
	else:
		di[state]={}	
		di[state][city]=1
		    	    
for state in di:
	total_num_cities = 0
	print(state)
	for city in di[state]:
		print(city, di[state][city])
		total_num_cities += di[state][city]
	print(state, total_num_cities)