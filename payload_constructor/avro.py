import json
import gzip
import os
import copy
from fastavro import reader, json_writer
import sys
import ast

# fname = sys.argv[1]

# dir_from = f"/data/cosmos-extract/data/{fname}"
dir_to = f"/data/event-hub-avro_converted"

reps = []
with open("repeat.txt") as infile:
    reps = infile.read().split('\n')


for file_name in reps:
    f = file_name
    file_name = f.split("/")[-1]
    if os.path.isfile(f) and file_name:
        try:
            with open(f'{f}',mode='rb') as thefile:
                
                content = reader(thefile)
                
                keys = []
                for payload in content:
                    if payload:
                                                
                        try:
                            c_payload = json.loads(payload["Body"].decode())
                        except ValueError as e:
                            h = open("./error.txt", "a")
                            h.write(f"{file_name} --> {e} \n")
                            h.close()
                        else:
                            if "end_device_ids" in c_payload:
                                app_id = c_payload["end_device_ids"]["application_ids"]["application_id"]
                                date = str(c_payload["received_at"]).split("T")[0]
                                dir_to_c = f"{dir_to}/{app_id}"
                                if not os.path.isdir(dir_to_c):
                                    os.makedirs(dir_to_c)
                                h = open(f"{dir_to_c}/{app_id}-{date}.json", "a")
                                h.write(str(f"{c_payload}\n"))
                                h.close()
                            else:
                                h = open("./error.txt", "a")
                                h.write(f"{file_name} --> payloads error \n")
                                h.close()
                                break
                
        except gzip.BadGzipFile:
            f = open("./error.txt", "a")
            f.write(f"{file_name} --> not a gzip file \n")
            f.close()

