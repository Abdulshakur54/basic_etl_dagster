import time
import pandas as pd
from pymongo.mongo_client import MongoClient
from ..resources import MySQLResource, MongoDBResource


def clean_data(data):
    data.dropna(inplace=True) #remove rows with null values
    return data

def append_processing_time(data):
    current_time_struct = time.gmtime(time.time())
    current_time = time.strftime("%Y:%m:%d, %H:%M:%S", current_time_struct)
    data['INSERTED_AT'] = current_time
    return data

def etl_pipeline(data, mongodb_conn):
    mongodb_conn.basic_etl.diamonds.insert_many(data.to_dict('records'))

def extract_data(mysql_cursor):
    results = get_diamonds(mysql_cursor)
    df = pd.DataFrame(results, columns = ['id','carat','cut','color','clarity','depth','tab', 'price','x','y','z'])
    return df 
 
def get_diamonds(mysql_cursor):
    limit = 5
    mysql_cursor.execute(f'SELECT id,carat,cut,color,clarity,depth,tab,price,x,y,z  from diamonds where flag = %s LIMIT %s' , (0, limit))
    results = mysql_cursor.fetchall()
    return results  
 