#!/usr/bin/env python3

import sys , json , math
import requests

LAT = float(sys.argv[1])
LNG = float(sys.argv[2])
D = float(sys.argv[3])

for x in sys.stdin:
    data = json.loads(x)
    lat = float(data["Start_Lat"])
    lng = float(data["Start_Lng"])

    
    if(not(math.isnan(lat) or math.isnan(lat))):
        dist = math.sqrt((LAT - lat)**2 + (LNG - lng)**2)

        if (dist <= D):
            payload = {
                "latitude": lat,    
                "longitude": lng
                }

            json_res = requests.post(url='http://20.185.44.219:5000/', json = payload)
            fetch = json_res.json()

            print(f'{fetch["state"]},{fetch["city"]},1')