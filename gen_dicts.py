import os
import json

with open("sectors.json", "w") as s:
	cnt = 0
        sectors = dict()
        for j in range(0, 1080, 120):
            for i in range(0, 1920, 120):
                coord = [i, j]
                arr = [i + 120, j + 120]
                sectors["sec_{}".format(cnt)] = [coord, arr]
                cnt += 1
        obj = json.dumps(sectors, indent=4)
	print >> s, obj
        

with open('temperature.json', 'w') as t:
        temps = dict()
        cnt = 1
        for i in range(0, 2000000, 100):
            temps[i] = cnt
            cnt += 1
        obj = json.dumps(temps, indent=4)
	print >> t, obj

