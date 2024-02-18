from basic_etl_dagster.assets.assets import tbl_diamonds, coll_diamonds
import pytest
from unittest.mock import patch, Mock, call
import pandas as pd

@pytest.fixture
@patch('basic_etl_dagster.assets.MySQLResource')
def mysql_resource(mysql_res):
    return mysql_res


@pytest.fixture
@patch('basic_etl_dagster.assets.MongoDBResource')
def mongodb_resource(mongodb_res):
    return mongodb_res

@pytest.fixture
def good_data_df():
    return pd.DataFrame({
        'id':[1,2,3],
        'carat': [0.23,0.21,0.24],
        'cut':  ['Ideal', 'Premium', 'Good'],
        'color': ['E', 'E', 'I']
    })
    
    
    
@pytest.fixture
def bad_data_df():
    return pd.DataFrame({
        'id': [1,2,3],
        'carat': [0.23,0.21,0.24],
        'cut':  [None, 'Premium', 'Good'],
        'color': ['E', 'E', 'I']
    })
    


def test_tbl_diamonds(mysql_resource):
    tbl_diamonds(mysql_resource)
    mysql_resource.connect.assert_called_with()
    execute = mysql_resource.connect.return_value.cursor.return_value.execute
    mock_calls = execute.mock_calls
    expected = [call('CREATE DATABASE IF NOT EXISTS basic_etl'), call('use basic_etl'), call("""
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
        """), call("SELECT count(id) as counter from diamonds")]
    print("how are you")
    expected == mock_calls[:5]
    
@patch('basic_etl_dagster.assets.extract_data')
def test_coll_diamonds(extract_data, mysql_resource, mongodb_resource, good_data_df, bad_data_df):
    extract_data.return_value = bad_data_df
    coll_diamonds(mysql_resource, mongodb_resource)
    mysql_mock_calls = mysql_resource.mock_calls
    mongodb_mock_calls = mongodb_resource.mock_calls
    mongodb_expected = [call.connect(), call.connect().basic_etl.diamonds.insert_many(good_data_df.to_dict('records'))]
    mysql_expected_first_three_calls = [call.connect(), call.cursor(), call.execute("use basic_etl")]
    mysql_expected_last_call = [call.connect().commit()]
    mongodb_expected == mongodb_mock_calls
    mysql_expected_first_three_calls == mysql_mock_calls[:3]
    mysql_expected_last_call == mysql_mock_calls[-1]
   
    

    

  
 

  
    
    
 


    
    