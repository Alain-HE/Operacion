import mysql.connector
import os
import pandas as pd
import numpy as np
from datetime import datetime

db = mysql.connector.connect(
    host="",
    user="",
    passwd="",
    database=""
)

cursor = db.cursor()
#cursor.execute("CREATE DATABASE testdatabase")
#cursor.execute("CREATE TABLE test (name varchar(50) NOT NULL, created datetime NOT NULL, gender ENUM('M','F','O') NOT NULL, id int PRIMARY KEY NOT NULL AUTO_INCREMENT)")
# cursor.execute("INSERT INTO users (name, age) VALUES (%s,%s)", ("Adri", 21))


# cursor.execute("INSERT INTO test (name, created, gender) VALUES (%s,%s,%s)", ("Adri", datetime.now(), "F"))
# cursor.execute("INSERT INTO test (name, created, gender) VALUES (%s,%s,%s)", ("Joe", datetime.now(), "M"))
# cursor.execute("INSERT INTO test (name, created, gender) VALUES (%s,%s,%s)", ("Alain", datetime.now(), "M"))
# cursor.execute("INSERT INTO test (name, created, gender) VALUES (%s,%s,%s)", ("Héctor", datetime.now(), "M"))

# cursor.execute("SELECT id, name FROM test WHERE gender = 'F' ORDER BY id ASC")
# for x in cursor:
#     print(x)

# cursor.execute("ALTER TABLE test ADD COLUMN food varchar(50) NOT NULL")
# cursor.execute('DESCRIBE test')
# print(cursor.fetchall())

# cursor.execute("ALTER TABLE test DROP age")

cursor.execute("ALTER TABLE test CHANGE name first_name varchar(50) NOT NULL")
cursor.execute('DESCRIBE test')
print(cursor.fetchall())