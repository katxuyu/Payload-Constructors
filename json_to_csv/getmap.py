from readconfig import *
import psycopg2 as pg
import pandas as pd
from sqlalchemy import create_engine

params = read_params()

host = params.get('DEVICE_MAP_DB', 'host')
database = params.get('DEVICE_MAP_DB', 'database')
user = params.get('DEVICE_MAP_DB', 'username')
password = params.get('DEVICE_MAP_DB', 'password')
port = params.get('DEVICE_MAP_DB', 'port')
table = params.get('DEVICE_MAP_DB', 'table')

def get_map():
    alchemyEngine   = pg.connect(f"host={host} port={port} dbname={database} user={user} password={password}")
    return pd.read_sql_query(f'SELECT type, type, name, id FROM {table}',con=alchemyEngine)

