# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 16:14:20 2022

@author: DB
"""

# Import module
import sqlite3
import numpy as np
import pandas as pd
# import os
# import re
#-----------------------------------------------------------------------------
# GEnerate data
#-----------------------------------------------------------------------------
#generate seeded random data Matrix1
np.random.seed(seed=11)
Matrix1 = np.random.randint(100, size=(10, 23))

#generate seeded random data Matrix2
np.random.seed(seed=13)
Matrix2 = np.random.randint(100, size=(10, 23))
# print(Matrix)

#convert numpy 2D array to Pandas dataframe
df1 = pd.DataFrame(Matrix1)
df2 = pd.DataFrame(Matrix2)

#convert Pandas dataframe to list of tuple for inserting into database
record1=[tuple(i) for i in df1.values.tolist()]
record2=[tuple(i) for i in df2.values.tolist()]
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Create SQLite3 database
#-----------------------------------------------------------------------------
# Connecte to sqlite
# conn = sqlite3.connect('OT.db') # create a database file
conn = sqlite3.connect(':memory:') # create a database in memory

# Create a cursor object using the cursor() method
cursor = conn.cursor()

# Create a table called OT (Optical Test)
tableName = 'OT'

createTable = '''
CREATE TABLE IF NOT EXISTS %s (
          Mode             VARCHAR,
          Cavity           INTEGER,
          Freqm            INTEGER,
          PMs              INTEGER,
          PMm              INTEGER,
          pcError          DECIMAL,
          EMBm             DECIMAL,
          EMBs             DECIMAL,
          PPs              INTEGER,
          Ps               INTEGER,
          PMm2             DECIMAL,
          SD               DECIMAL,
          Tolerance        DECIMAL,
          pcTol_Mean_ratio DECIMAL,
          MeanP3SD         DECIMAL,
          MeanN3SD         DECIMAL,
          Max              DECIMAL,
          Min              DECIMAL,
          pcError2         DECIMAL,
          OOB              INTEGER,
          Size             INTEGER,
          Marki            INTEGER,
          pc5              INTEGER
          )
          ''' %tableName

cursor.execute(createTable)

#-----------------------------------------------------------------------------
# Create as many '?,' as there are fields in the table
#-----------------------------------------------------------------------------
# a=''
# w=23
def fieldNum(a, w):
    # for i in range(w):
    #     a +='?,'
    #     a=a.join([a+'?,' for i in range(w)])[:-1]
    return a.join([a+'?,' for i in range(w)])[:-1]

a = fieldNum('', 23)

#-----------------------------------------------------------------------------
# Insert OT data into the table with one-shot
#-----------------------------------------------------------------------------
# cursor.executemany('INSERT INTO '+tableName+' VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);',record1);
cursor.executemany('INSERT INTO '+tableName+' VALUES('+a+');',record1);
cursor.executemany('INSERT INTO '+tableName+' VALUES('+a+');',record2);

# Commit changes in the database	
conn.commit()

# Print information about how many records inserted into the table
print('We have inserted', cursor.rowcount*2, 'records to the table.')

# Display data in the database table
print("Data Inserted in the table:")
data=cursor.execute('''SELECT * FROM %s'''%tableName)
for row in data:
    print(row)

# Closing the database connection
conn.close()
#-----------------------------------------------------------------------------