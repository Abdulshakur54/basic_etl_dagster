from dagster import asset
from .assets_functions import clean_data, append_processing_time, etl_pipeline, extract_data, get_diamonds
from ..resources import MySQLResource, MongoDBResource


@asset
def tbl_diamonds(mysql_resource: MySQLResource):
    """Loads data to diamonds table from a CSV file"""
    mysql_connect = mysql_resource.connect()
    mysql_cursor = mysql_connect.cursor()
    mysql_cursor.execute('CREATE DATABASE IF NOT EXISTS basic_etl')
    mysql_cursor.execute('use basic_etl')
    mysql_cursor.execute(
       """
        CREATE TABLE IF NOT EXISTS diamonds (
            id INT(11) AUTO_INCREMENT PRIMARY KEY,
            carat FLOAT,
            cut VARCHAR(20),
            color CHAR,
            clarity VARCHAR(20),
            depth FLOAT,
            tab INT,
            price FLOAT,
            x FLOAT,
            y FLOAT,
            z FLOAT
        );
        """
    )

    mysql_cursor.execute("SELECT count(id) as counter from diamonds")
    count = mysql_cursor.fetchone()[0]
    if count == 0:
        mysql_cursor.execute(
            """
                LOAD DATA INFILE  '/var/lib/mysql-files/diamond.csv' 
                INTO TABLE diamonds 
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
            """
        )
        mysql_cursor.execute("ALTER TABLE diamonds ADD COLUMN flag INT(1) DEFAULT 0")



    """
    this is to fix a bug, all execution statement will run except the last one, that is one this is added to make it all run
    """
    mysql_cursor.execute('CREATE DATABASE IF NOT EXISTS basic_etl')

@asset(deps=["tbl_diamonds"])
def coll_diamonds(mysql_resource: MySQLResource,  mongodb_resource: MongoDBResource):
    """Data is extracted from tbl_diamonds cleaned and loaded into diamonds collection """
    mysql_conn = mysql_resource.connect()
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute("use basic_etl")
    mongodb_conn = mongodb_resource.connect()
    data = extract_data(mysql_cursor)
    cleaned_data = clean_data(data)
    process_data = append_processing_time(cleaned_data)
    try:
        mysql_conn.autocommit = False
        etl_pipeline(process_data, mongodb_conn)
        diamonds = get_diamonds(mysql_cursor)
        ids = [diamond[0] for diamond in diamonds]
        updated_ids = tuple(ids)
        placehoders = ', '.join(('%s' for u_id in updated_ids))
        mysql_cursor.execute(f"UPDATE diamonds SET flag = %s WHERE id in ({placehoders})", (1,) + updated_ids)
        mysql_conn.commit()
    except Exception as e:
        mysql_conn.rollback()
        raise(e)
