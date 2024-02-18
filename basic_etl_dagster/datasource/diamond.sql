CREATE DATABASE basic_etl;
USE basic_etl;
CREATE TABLE diamonds (
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
    z FLOAT,
	created DATETIME DEFAULT CURRENT_TIMESTAMP,
	flag INT DEFAULT 0
);


LOAD DATA INFILE  '/var/lib/mysql-files/diamond.csv' 
INTO TABLE diamonds 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
SET created = CURRENT_TIMESTAMP;