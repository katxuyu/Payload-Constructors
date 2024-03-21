from flask import Flask, request
from datetime import datetime
import psycopg2
from dateutil import parser 

app = Flask(__name__)

di_filters = [""]

def db_connect():
    conn = psycopg2.connect(database="postgres",
                            host="",
                            user="",
                            password="",
                            port="5432")
    
    return conn

def insert_to_db(conn, data):
    if conn.closed == 1:
        conn = db_connect()
    try:
        sql = """INSERT INTO bls.bls_data(device_id, received_at, rssi, timestamp, mac)
                VALUES(%s, %s, %s, %s, %s)"""
        cur = conn.cursor()
        cur.execute(sql, data)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"ERROR {e}")
        if conn.closed == 0:
            conn.close()
        


conn = db_connect()

@app.route('/bls_payloads', methods=["POST"])
def index():
    payload = request.json
    print(payload)
    device_id = payload["end_device_ids"]["device_id"]
    if device_id in di_filters:
        uplink_message = payload["uplink_message"]
        received_at = parser.parse(uplink_message["received_at"])
        if "decoded_payload" in uplink_message and "scan_data" in uplink_message["decoded_payload"]:
            scan_data = uplink_message["decoded_payload"]["scan_data"]
            for data in scan_data:
                rssi = int(str(data["rssi"]).replace("dBm", ""))
                timestamp = datetime.strptime(data["timestamp"], "%Y/%m/%d %H:%M:%S")
                mac = data["mac"]
                save_data = (device_id, received_at, rssi, timestamp, mac,)

                insert_to_db(conn, save_data)

    return "OK", 200

            
    
