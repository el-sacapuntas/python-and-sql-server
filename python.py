import pyodbc
import pandas as pd
import sqlalchemy
import os  
from dotenv import load_dotenv

load_dotenv() 

USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')

#pandas loads to system memory - read in chunks to optimise memory usage
tradechunks = pd.read_json('data/trades.json', lines=True, chunksize=50)

#created dsn sql_server (this engine chosen as it is currently on system for separate project)
engine = sqlalchemy.create_engine("mssql+pyodbc://"+USER+":"+PASSWORD+"@sql_server")

for chunk in tradechunks:
    #convert to sql serrver format
    chunk['event_timestamp'] = chunk['event_timestamp'].str.replace(' UTC','')
    chunk['event_timestamp'] = pd.to_datetime(chunk['event_timestamp'], format='%Y/%m/%d %H:%M:%S')
    chunk.to_sql('trades', engine, if_exists='append', index=False)

#pandas loads to system memory - read in chunks to optimise memory usage
valuechunks = pd.read_json('data/valuedata.json', lines=True, chunksize=50)

for chunk in valuechunks:
    #convert to sql serrver format
    chunk['when_timestamp'] = chunk['when_timestamp'].str.replace(' UTC','')
    chunk['when_timestamp'] = pd.to_datetime(chunk['when_timestamp'], format='%Y/%m/%d %H:%M:%S')
    chunk.to_sql('value_data', engine, if_exists='append', index=False)
