#!/usr/bin/env python3

import sys , json , math
from datetime import datetime

for x in sys.stdin:
	data = json.loads(x)
	if(
	not(math.isnan(data["Severity"]) or (data["Sunrise_Sunset"] != data["Sunrise_Sunset"]) or math.isnan(data["Visibility(mi)"]) or math.isnan(data["Precipitation(in)"]) 
	or (data["Weather_Condition"] != data["Weather_Condition"]) or (data["Description"] != data["Description"])) 
	and data["Severity"] >= 2 
	and str(data["Sunrise_Sunset"]).lower() == "night" 
	and data["Visibility(mi)"] <= 10 
	and data["Precipitation(in)"] >= 0.2 
	and("heavy snow" == str(data["Weather_Condition"]).lower().strip() or "thunderstorm" == str(data["Weather_Condition"]).lower().strip() or "heavy rain" == 
	str(data["Weather_Condition"]).lower().strip() or "heavy rain showers" == str(data["Weather_Condition"]).lower().strip() or "blowing dust" == str(data["Weather_Condition"]).lower().strip()) 
	and("lane blocked" in data["Description"].lower() or "shoulder blocked" in data["Description"].lower() or "overturned vehicle" in data["Description"].lower()) 
	):
		date = (data["Start_Time"].split('.')[0] if '.' in data["Start_Time"] else data["Start_Time"])
		time = datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
		print(f"{time.hour},1")
