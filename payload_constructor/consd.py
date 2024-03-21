import json
import gzip
import os
import sys

# dir_from = "./payloads-0316-and-earlier"
# dir_to = "./payloads-0316-and-earlier_converted"

fname = sys.argv[1]

dir_from = f"/data/payloads-2021-202201-gcp/{fname.split('-')[0]}-{fname.split('-')[1]}/{fname}"
dir_to = "/data/payloads-2021-202201-gcp_converted"



reps = []
with open("repeat.txt") as infile:
    reps = infile.read().split('\n')
    for file_name in reps:
        g = os.path.join(dir_to, file_name)
        if os.path.isfile(g) and file_name:
            
            os.remove(g)

if not os.path.isdir(dir_to):
    os.makedirs(dir_to)


for file_name in os.listdir(dir_from):
#for file_name in reps:
    f = os.path.join(dir_from, file_name)
    if os.path.isfile(f) and file_name:
        try:
            with open(f'{f}',mode='r') as thefile:
                
                content = str(thefile.read())
                
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
                            c_payload = c_payload["payload"]
                            if "payload_fields" in c_payload:
                                
                                data = {
                                    "end_device_ids": {
                                        "device_id": c_payload["dev_id"],
                                        "application_ids": {
                                            "application_id": c_payload["app_id"]
                                        }
                                    },
                                    "received_at": c_payload["metadata"]["time"],
                                    "uplink_message": {
                                        "decoded_payload": c_payload["payload_fields"],
                                        "received_at": c_payload["metadata"]["time"]
                                    }
                                }
                                cfile_name = file_name.split("T")[0]
                                h = open(f"{dir_to}/{cfile_name}.json", "a")
                                h.write(str(f"{data}\n"))
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

