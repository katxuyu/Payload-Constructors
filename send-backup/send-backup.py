import time
import requests
import json
import sys
from time import sleep
from datetime import datetime


start_time = datetime.strptime('2022-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#end_time = datetime.strptime('2022-07-22 15:00:00', '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime('2022-10-04 00:00:00', '%Y-%m-%d %H:%M:%S')

base_url = ""

#2022-05-09T17:46:40

url = base_url + sys.argv[1]
filename = sys.argv[2]
counter = 0
max_time = start_time
max_time_str = ""
if len(sys.argv)>3:
    start_counter = int(sys.argv[3])
else:
    start_counter = 0

with open(filename, "r") as file:
    for line in file:
        counter += 1
#        print(counter)
        payload = json.loads(line)
        temp_time_str = payload["received_at"][0:19] #2022-01-10T00:16:41.539168451Z
#        print(temp_time_str)
        print(filename, counter, temp_time_str)
        result = requests.post(url, json=payload)
        print(result)

        
#        temp_time = datetime.strptime(temp_time_str, '%Y-%m-%dT%H:%M:%S')
#        if counter > start_counter and temp_time >= start_time and temp_time <= end_time: # and payload["end_device_ids"]["device_id"] in dev_ids:
#            if temp_time > max_time:
#                max_time = temp_time
#                max_time_str = temp_time_str
#            print(filename, counter, temp_time_str)
#            for url in urls:
#            result = requests.post(url, json=payload)
#            print(result)
#            sleep(0.01)
#            break

        #print(payload)
        #result = requests.post(url, json=payload)
        #sleep(0.01)

print("Max counter: " + str(counter))
print("Max time: " + max_time_str)
print("Finished processing: " + filename)
