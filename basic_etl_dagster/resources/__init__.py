from dagster import ConfigurableResource
import mysql.connector
from  mysql.connector.connection_cext import CMySQLConnection
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class MySQLResource(ConfigurableResource):

    host: str
    username: str
    password: str
    
    def connect(self) ->CMySQLConnection:
        mysql_conn = mysql.connector.connect(
            host=self.host,
            user=self.username,
            password=self.password
        )
        return mysql_conn

class MongoDBResource(ConfigurableResource):
    
    host: str
    username: str
    password: str
    
    def connect(self) -> MongoClient:
        conn_url = f"mongodb://{self.username}:{self.password}@{self.host}/?retryWrites=true&w=majority"
        return MongoClient(conn_url, server_api = ServerApi('1'))
        
