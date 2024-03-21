import json
import gzip
import os

import sys

fname = sys.argv[1]

dir_from = f"./{fname}"
dir_to = f"/data/main"


for file_name in os.listdir(dir_from):
    f = os.path.join(dir_from, file_name)
    if os.path.isfile(f) and file_name:
        try:
            with gzip.open(f'{f}',mode='r') as thefile:
                
                content = str(thefile.read().decode("utf-8-sig"))
                
                payloads = content.split("\n")
                keys = []
                for payload in payloads:
                    if payload:
                        try:
                            c_payload = json.loads(str(payload).replace('"',"").replace("'", '"').replace('\\', '\\\\'))
                        except ValueError as e:
                            h = open("./error.txt", "a")
                            h.write(f"{file_name} --> {e} \n")
                            h.close()
                            print(payload)
                           
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

