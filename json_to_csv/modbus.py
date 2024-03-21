import json
import gzip
import os
import pandas as pd
import re
from dateutil import parser
import csv
from datetime import datetime, timezone

dir_from = "."
dir_to = "outs"
dmap = pd.read_csv("deviceIdmap.csv")
dmap = dmap.fillna('')
conv_names = list(dmap.iloc[:, 2])
webhooks = list(dmap.iloc[:, 3])
device_names = list(dmap.iloc[:, 4])
device_ids = list(dmap.iloc[:, 5])
c_dmap = {}

ts_dic = pd.read_csv("ts_kv_dictionary.csv", index_col=0, header=None, squeeze=True).to_dict()

for cn, wh, dn, di in zip(conv_names, webhooks, device_names, device_ids):
    if "ESN" in dn:
        dn = dn.split(" ")[1] 
    if cn in c_dmap:
        c_dmap[cn].append([wh, dn, di])
    else:
        c_dmap[cn] = [[wh, dn, di]]


valid_payload = ["voltage_L1", "voltage_L2", "voltage_L3", "voltage_L4", "voltage_L5", "voltage_L6", "voltage_L7", "voltage_L8","voltage_L9", 
		"current_L1", "current_L2", "current_L3", "current_L4", "current_L5", "current_L6", "current_L7", "current_L8", "current_L9",
		"Active_Power_L1", "Active_Power_L2", "Active_Power_L3", "Active_Power_L4", "Active_Power_L5", "Active_Power_L6", "Active_Power_L7", "Active_Power_L8", "Active_Power_L9",
		"Apparent_Power_L1", "Apparent_Power_L2", "Apparent_Power_L3",
		"Power_Factor_L1", "Power_Factor_L2", "Power_Factor_L3",
		"current_Total", "Active_Power_Total", "Apparent_Power_Total", "Reactive_Power", "Power_Factor_Total",
		"Frequency", "Active_Energy_Import", "Active_Energy_Export", "Reactive_Energy_Import", "Reactive_Energy_Export",
		"voltage_L1L2", "voltage_L2L3", "voltage_L3L1",
		"current_N",
		"Active_Energy_Net",
		]

def process(payload, fn):
    c_payload = json.loads(payload)
    device_id = c_payload["end_device_ids"]["device_id"]
    decoded_payload = c_payload["uplink_message"]["decoded_payload"]
    
    creation_date = c_payload["uplink_message"]["received_at"]
    fn = fn.replace(".json.gz", "").split("-")
    n_mon = f"{fn[-3]}{fn[-2]}"
    fn = fn[:len(fn) - 3]
    fn = '-'.join(str(item) for item in fn)
    dt = [None, None, None, None]
    
            
    for m in c_dmap[fn]:
        if m[0] == fn:
            if m[1] in device_id:
                dt[0] = m[2]

        
        dt[1] = parser.parse(creation_date)
        dt[1] =  int(dt[1].replace(tzinfo=timezone.utc).timestamp() * 1000)

    for p in decoded_payload:
        ddt = []
        ddt.extend(dt)
        if p in valid_payload and ddt[3] == None:
            ddt[3] = decoded_payload[p]
        if p in ts_dic and ddt[2] == None:
            ddt[2] = ts_dic[p]
            
        if None in ddt:
            if ddt[0] == None:
                f = open("errors/ntt_id_not_in_the_list.txt", "a")
            elif ddt[1] == None:
                f = open("errors/no_creation_date.txt", "a")
            elif ddt[2] == None:
                f = open("errors/payload_name_not_in_ts_dict.txt", "a")
            elif ddt[3] == None: 
                f = open("errors/invalid_payload_name.txt", "a")
            f.write(f"{c_payload}\n")
            f.close()
        else:
            with open(os.path.join(dir_to,f'{fn}-{p}-{ddt[2]}-{n_mon}.csv'), 'a', encoding='UTF8') as f:
                writer = csv.writer(f)
                writer.writerow(ddt)

            # with open(os.path.join(dir_to,f'{fn}-{p}-{ddt[2]}-{n_mon}.csv'), 'a', encoding='UTF8') as f:
            #     writer = csv.writer(f)
            #     writer.writerow(ddt)
    
    
    
        
        


for file_name in os.listdir(dir_from):
#for file_name in reps:
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


