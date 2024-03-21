import requests
import json
import csv

def retrieve_data():
    headers = {"Accept": "application/json, text/plain, */*",
               "Authorization": ""}
    res = requests.get("", headers=headers)
    return res.json()

data = []
def get_device_count():
    applications_data = retrieve_data()
    
    for app in applications_data["applications"]:
        app_id = app["ids"]["application_id"]
        headers = {"Authorization": ""}
        res = requests.get(f"https://packetworx.eu1.cloud.thethings.industries/api/v3/applications/{app_id}/devices", headers=headers)
        devices =  res.json()
        if "end_devices" in devices:
            devices_count = len(devices["end_devices"])
            data.append([app_id, devices_count])
        else:
            data.append([app_id, 0])
    return "DONE!"
    
print(get_device_count(), data)
with open('GFG.csv', 'w') as f:
      
    # using csv.writer method from CSV package
    write = csv.writer(f)
      
    write.writerow(["application", "device_count"])
    write.writerows(data)
