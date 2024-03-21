import json
import gzip
import os
from timeit import repeat

dir_from = "./payloads-0316-and-earlier"
dir_to = "./payloads-0316-and-earlier_converted"
reps = []
with open("repeat.txt") as infile:
    reps = infile.read().split('\n')
    for file_name in reps:
        g = os.path.join(dir_to, file_name)
        if os.path.isfile(g) and file_name:
            
            os.remove(g)

for file_name in os.listdir(dir_from):
#for file_name in reps:
    f = os.path.join(dir_from, file_name)
    if os.path.isfile(f) and file_name:
        try:
            with gzip.open(f'{f}',mode='r') as thefile:
                
                content = str(thefile.read().decode("utf-8"))
                
                payloads = content.split("\n")
                keys = []
                for payload in payloads:
                    if payload:
                        try:
                            c_payload = json.loads(payload)
                        except ValueError as e:
                            h = open("./error.txt", "a")
                            h.write(f"{file_name} --> {e} \n")
                            h.close()
                            break
                        if "key" in c_payload and c_payload['key'] not in keys and not keys:
                            keys.append(c_payload['key'])
                        if "key" in c_payload and c_payload['key'] in keys and "payload" in c_payload:
                            h = open(f"{dir_to}/{file_name}".replace(".gz",""), "a")
                            h.write(str(f"{c_payload['payload']}\n"))
                            h.close()
                        elif "key" in c_payload and c_payload['key'] not in keys:
                            pass
                        else:
                            h = open("./error.txt", "a")
                            h.write(f"{file_name} --> payloads error \n")
                            h.close()
                            break
        except gzip.BadGzipFile:
            f = open("./error.txt", "a")
            f.write(f"{file_name} --> not a gzip file \n")
            f.close()

