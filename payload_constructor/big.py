
import json
import gzip
import os
from zipfile import ZipFile
import sys

fname = sys.argv[1]

# dir_from = "/data/from-cosmosdb"
# dir_to = f"/data/from-cosmosdb_converted/{fname}"

dir_from = "./from-cosmosdb"
dir_to = "./from-cosmosdb_converted"



if not os.path.isdir(dir_to):
    os.mkdir(dir_to)


def process(content, file_name):
    payloads = content.split("\n")
    keys = {
            "end_device_ids": {
                "device_id": "",
                "application_ids": {
                    "application_id": ""
                }
            },
            "received_at": "",
            "uplink_message": {
                "decoded_payload": {},
                "received_at": ""
            }
        }

    for payload in payloads:
        if payload:
            try:
                c_payload = json.loads(payload)
            except ValueError as e:
                h = open("./error.txt", "a")
                h.write(f"{file_name} --> {e} \n")
                h.close()
                return
             
            # if "received_at" in c_payload:
            #     date = str(c_payload["received_at"]).split("T")[0]
            #     h = open(f"{dir_to}/{file_name}".replace(".json.zip",f"-{date}.json"), "a")
            #     h.write(f"{c_payload}\n")
            #     h.close()
            else:
                h = open("./error.txt", "a")
                h.write(f"{file_name} --> payloads error \n")
                h.close()
                return

#for file_name in os.listdir(dir_from):
#for file_name in reps:
for file_name in [f"{fname}.json.zip"]:
    f = os.path.join(dir_from, file_name)
    if os.path.isfile(f) and file_name:
        try:
            with ZipFile(f'{f}',mode='r') as thezip:
                with thezip.open(thezip.filelist[0],mode='r') as thefile:
                    count = 0
                    while True:
                        count += 1
                        #if count >= read_from and count <= read_to:
                        content = str(thefile.readline().decode("utf-8-sig"))
                        if not content:
                            print(f"NO MORE CONTENT: {count}")
                            break
                        process(content, file_name)
                        #elif count > read_to:
                        #    print("READ END")
                        #    break
                        
                        
        except gzip.BadGzipFile:
            f = open("./error.txt", "a")
            f.write(f"{file_name} --> not a gzip file \n")
            f.close()
    else:
        print(os.path.isfile(f), f, file_name)

def process(content, file_name):
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
                return
            if "received_at" in c_payload:
                date = str(c_payload["received_at"]).split("T")[0]
                #fn = f"{dir_to}/{file_name}".replace(".json.zip",f"-{date}.json.gz")
                h = open(f"{dir_to}/{file_name}".replace(".json.zip",f"-{date}.json"), "a")
                h.write(f"{c_payload}\n")
                h.close()
            else:
                h = open("./error.txt", "a")
                h.write(f"{file_name} --> payloads error \n")
                h.close()
                return

for file_name in os.listdir(dir_from):
#for file_name in reps:
    f = os.path.join(dir_from, file_name)
    if os.path.isfile(f) and file_name:
        try:
            with ZipFile(f'{f}',mode='r') as thezip:
                with thezip.open(thezip.filelist[0],mode='r') as thefile:
                    count = 0
                    while True:
                        count += 1
                        #if count >= read_from and count <= read_to:
                        content = str(thefile.readline().decode("utf-8-sig"))
                        if not content:
                            print(f"NO MORE CONTENT: {count}")
                            break
                        process(content, file_name)
                        #elif count > read_to:
                        #    print("READ END")
                        #    break
                        
                        
        except gzip.BadGzipFile:
            f = open("./error.txt", "a")
            f.write(f"{file_name} --> not a gzip file \n")
            f.close()

