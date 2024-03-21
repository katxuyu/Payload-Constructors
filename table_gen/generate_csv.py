import os
import csv
import json
import gzip

dir_from = "."

pas = 0
def process(payload, fn):
    global pas
    c_payload = json.loads(payload)
    device_id = c_payload["end_device_ids"]["device_id"]
    table = [["adv_data", "adv_len", "mac", "rssi", "utc_time", "device_id"]] if pas == 0 else []
    if "decoded_payload" in c_payload["uplink_message"]:
        if "scan_data" in c_payload["uplink_message"]["decoded_payload"]:
            scan_data = c_payload["uplink_message"]["decoded_payload"]["scan_data"]
            
            for data in scan_data:
                my_list = list(data.values())
                my_list.append(device_id)
                table.append(my_list)
            for t in table:
                with open(os.path.join(dir_from,fn.replace(".json.gz", ".csv")), 'a', encoding='UTF8') as f:
                    writer = csv.writer(f)
                    writer.writerow(t)
                    pas = 1

for file_name in os.listdir(dir_from):
    f = os.path.join(dir_from, file_name)
    if os.path.isfile(f) and file_name:
        try:
            with gzip.open(f'{f}',mode='r') as thefile:
                count = 0
                while True:
                    count += 1
                    content = str(thefile.readline().decode("utf-8-sig"))
                    if not content:
                        print(f"NO MORE CONTENT: {count}")
                        break
                    process(content, file_name)
                    if  count == 10000 or count == 50000 or count == 25000 or count == 30000 or count == 100000:
                        print(count)
                    
                    

        except Exception as e:
            print(e)