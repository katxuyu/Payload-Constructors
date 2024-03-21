import json
import gzip
import os
import pandas as pd
import re
from dateutil import parser
import csv
from datetime import datetime, timezone
from multiprocessing import Pool
import yaml
from getmap import get_map

os.system("mkdir to_be_processed errored data_errors converted outs")

dir_from = "to_be_processed"
dir_temp = "temp"
dir_to = "outs"
dir_data_err = "data_errors"
dir_converted = "converted"
dir_errored = "errored"
dir_yamls = "yamls"

# dmap = pd.read_csv("deviceIdmap.csv")
# dmap = dmap.fillna('')
dmap = get_map()
conv_names = list(dmap.iloc[:, 0])
webhooks = list(dmap.iloc[:, 1])
device_names = list(dmap.iloc[:, 2])
device_ids = list(dmap.iloc[:, 3])

c_dmap = {}

code_files = ["ts_kv_dictionary.csv", "cbremodbus.txt", "modbus.py", "threaded.py", "deviceIdmap"]

ts_dic = pd.read_csv("ts_kv_dictionary.csv", index_col=0, header=None, squeeze=True).to_dict()

for cn, wh, dn, di in zip(conv_names, webhooks, device_names, device_ids):
    if "ESN" in dn:
        dn = dn.split(" ")[1]
    if cn in c_dmap and (wh == '' or wh == 'null'):
        c_dmap[cn].append([wh, dn, di])
    elif wh in c_dmap:
        c_dmap[wh].append([cn, dn, di])
    else:
        c_dmap[wh] = [[cn, dn, di]]


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


#     print(valid_payload["applications"]["cbre"]["voltage_L1"])
        


def process(arg):
    payload = arg[0]
    fn = arg[1]
    try:
        c_payload = json.loads(payload)
    except:
        c_payload = payload.replace('"', "") if '"' in payload else payload
        c_payload = payload.replace("'", '"')
        #print("WEEEEEEEEEEE", c_payload, "WOOOOOOOOOOOO")
        #c_payload = ast.literal_eval()
        c_payload = json.loads(c_payload)

    if "decoded_payload" not in c_payload["uplink_message"]:
        f = open(f"{dir_data_err}/no_decoded_payload.txt", "a")
        f.write(f"{c_payload}\n")
        f.close()
        return

    
    device_id = c_payload["end_device_ids"]["device_id"]
    decoded_payload = c_payload["uplink_message"]["decoded_payload"]

    
        
    
    creation_date = c_payload["uplink_message"]["received_at"]
    fn = fn.replace(".json.gz", "").split("-")
    n_mon = f"{fn[-3]}{fn[-2]}"
    partition = f"{fn[-3]}-{fn[-2]}-01T00:00:00Z"
    fn = fn[:len(fn) - 3]
    fn = '-'.join(str(item) for item in fn)
    dt = [None, None, None, None, None, None]

    app_id = c_payload["end_device_ids"]["application_ids"]["application_id"] if "application_ids" in c_payload["end_device_ids"] else fn
    
    if not os.path.isfile(f"yamls/{app_id}.yaml"):
        raise ValueError(f"{app_id}.yaml does not exist in")

    with open(f"yamls/{app_id}.yaml") as stream:
        valid_payload = yaml.safe_load(stream)
    
    
    
    # if fn not in c_dmap:
    #     raise Exception("device not founf in map")

    
    #print(c_payload)        
    for m in c_dmap[fn]:
        dt[0] = "DEVICE"

        if m[0] == fn:
            if m[1] in device_id:
                dt[1] = m[2]

        dt[3] = parser.parse(partition)
        dt[3] =  int(dt[3].replace(tzinfo=timezone.utc).timestamp() * 1000)

        dt[4] = parser.parse(creation_date)
        dt[4] =  int(dt[4].replace(tzinfo=timezone.utc).timestamp() * 1000)

    
        

    for p in decoded_payload:
        ddt = []
        ddt.extend(dt)
        
        if p in valid_payload[app_id] and ddt[5] == None:
            ddt[5] = decoded_payload[p]
        if p in valid_payload[app_id] and ddt[2] == None:
            ddt[2] = valid_payload[app_id][p]
            
        if None in ddt:
            if ddt[1] == None:
                f = open(f"{dir_data_err}/ntt_id_not_in_the_list/{arg[1].replace('.gz', '')}", "a")
            elif ddt[2] == None:
                f = open(f"{dir_data_err}/payload_name_not_in_ts_dict/{arg[1].replace('.gz', '')}", "a")
            elif ddt[3] == None: 
                f = open(f"{dir_data_err}/invalid_payload_name/{arg[1].replace('.gz', '')}", "a")
            elif ddt[4] == None:
                f = open(f"{dir_data_err}/no_creation_date/{arg[1].replace('.gz', '')}", "a")
            elif ddt[5] == None: 
                f = open(f"{dir_data_err}/no_payload/{arg[1].replace('.gz', '')}", "a")
            f.write(f"{c_payload}\n")
            f.close()
        else:
            with open(os.path.join(dir_temp, f'{fn}-{p}-{n_mon}.csv'), 'a', encoding='UTF8') as f:
                writer = csv.writer(f)
                writer.writerow(ddt)


            # with open(os.path.join(dir_to,f'{fn}-{p}-{ddt[2]}-{n_mon}.csv'), 'a', encoding='UTF8') as f:
            #     writer = csv.writer(f)
            #     writer.writerow(ddt)
    # except Exception as e:
    #     f = open("errors/process_error.txt", "a")
    #     f.write(f"{arg[1]}\n")
    #     f.close()
    
    
        
        
month_now = ""
files = sorted(os.listdir(dir_from))
files = list(filter(lambda a: ".gz" in a, files))


for idx, file_name in enumerate(files):
#for file_name in reps:
    f = os.path.join(dir_from, file_name)
    if os.path.isfile(f) and file_name:
        try:
            with gzip.open(f'{f}',mode='r') as thefile:
                count = 0
                t_count = 0
                to_process = []
                while True:
                    count += 1
                    t_count += 1
                    content = str(thefile.readline().decode("utf-8-sig"))
                    if content:
                        to_process.append([content,file_name])
                        
                    if  t_count == 20000 or not content:
                        #print(to_process[0][0])
                        with Pool(processes=os.cpu_count()) as pool:
                            known_words = set(pool.map(process, to_process))
                        print(count)
                        to_process = []
                        t_count = 0
                    if not content:
                        print(f"NO MORE CONTENT: {count}")
                        
                        os.system(f"for f in ./{dir_temp}/*; do cat ./{dir_temp}/$(basename  -- $f) >> ./{dir_to}/$(basename  -- $f); rm ./{dir_temp}/$(basename  -- $f); done ")
                        os.system(f"mv ./{dir_from}/{file_name} ./{dir_converted}/")
                        break
        
        except Exception as e:
            if "Not a gzipped file" not in str(e):
                if "Compressed file ended before the end-of-stream marker was reached" not in str(e):
                    if len(os.listdir(f"./{dir_temp}")) != 0:
                        os.system(f"rm -f ./{dir_temp}/*")
                    os.system(f"mv ./{dir_from}/{file_name} ./{dir_errored}/")
                else:
                    os.system(f"for f in ./{dir_temp}/*; do cat ./{dir_temp}/$(basename  -- $f) >> ./outs/$(basename  -- $f); rm ./{dir_temp}/$(basename  -- $f); done ")

                print("ERROR: ", e)


