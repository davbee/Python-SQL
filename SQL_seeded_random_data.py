# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 16:14:20 2022

@author: DB
"""

# Import module
import sqlite3
import numpy as np
import pandas as pd
import os
# import re
#-----------------------------------------------------------------------------
# Obtain data
#-----------------------------------------------------------------------------
print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)
print('basename:    ', os.path.basename(__file__))
print('dirname:     ', os.path.dirname(__file__))

# PWD = os.getcwd() #get current directory
# Account = PWD.split('\\')[2] #get the User account so that program can find directory path for different user accounts
# test data paths
# path1 = 'C:\\Users\\{}\\Documents\\Programming\\Python\\BSC\\SQL\\VSSdata\\PRECL2\\20210202_083029'.format(Account)
# path2 = 'C:\\Users\\{}\\Documents\\Programming\\Python\\BSC\\SQL\\VSSdata\\PRECL2\\20210202_090510'.format(Account)
# path1 = '.\\VSSdata\\PRECL2\\20210202_083029'.format(Account)
# path2 = '.\\VSSdata\\PRECL2\\20210202_090510'.format(Account)
# path1 = 'BSC\\SQL\\VSSdata\\PRECL2\\20210202_083029'
# path2 = 'BSC\\SQL\\VSSdata\\PRECL2\\20210202_090510'
# path1 = 'VSSdata\\PRECL2\\20210202_083029'
# path2 = 'VSSdata\\PRECL2\\20210202_090510'

# file paths
# fileName1 = 'VSS_PRECL2_OT_083029.csv'
# fileName2 = 'VSS_PRECL2_OT_090510.csv'

# Create Pandas dataframe from reading csv file's contents
# df = pd.read_csv(fName)
# df1 = pd.read_csv(path1+'/'+fileName1)
# df2 = pd.read_csv(path2+'/'+fileName2)

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
cursor.execute('CREATE TABLE IF NOT EXISTS ' + tableName +
          '(Mode            VARCHAR,'
          'Cavity           INTEGER,'
          'Freqm            INTEGER,'
          'PMs              INTEGER,'
          'PMm              INTEGER,'
          'pcError          DECIMAL,'
          'EMBm             DECIMAL,'
          'EMBs             DECIMAL,'
          'PPs              INTEGER,'
          'Ps               INTEGER,'
          'PMm2             DECIMAL,'
          'SD               DECIMAL,'
          'Tolerance        DECIMAL,'
          'pcTol_Mean_ratio DECIMAL,'
          'MeanP3SD         DECIMAL,'
          'MeanN3SD         DECIMAL,'
          'Max              DECIMAL,'
          'Min              DECIMAL,'
          'pcError2         DECIMAL,'
          'OOB              INTEGER,'
          'Size             INTEGER,'
          'Marki            INTEGER,'
          'pc5              INTEGER)')

# createTable = '''
# 'CREATE TABLE IF NOT EXISTS' %s
#           '(Mode            VARCHAR,'
#           'Cavity           INTEGER,'
#           'Freqm            INTEGER,'
#           'PMs              INTEGER,'
#           'PMm              INTEGER,'
#           'pcError          DECIMAL,'
#           'EMBm             DECIMAL,'
#           'EMBs             DECIMAL,'
#           'PPs              INTEGER,'
#           'Ps               INTEGER,'
#           'PMm2             DECIMAL,'
#           'SD               DECIMAL,'
#           'Tolerance        DECIMAL,'
#           'pcTol_Mean_ratio DECIMAL,'
#           'MeanP3SD         DECIMAL,'
#           'MeanN3SD         DECIMAL,'
#           'Max              DECIMAL,'
#           'Min              DECIMAL,'
#           'pcError2         DECIMAL,'
#           'OOB              INTEGER,'
#           'Size             INTEGER,'
#           'Marki            INTEGER,'
#           'pc5              INTEGER)'
#           '''%tableName
# cursor.execute(createTable)
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
print('We have inserted', cursor.rowcount, 'records to the table.')

# Display data in the database table
print("Data Inserted in the table:")
data=cursor.execute('''SELECT * FROM %s'''%tableName)
for row in data:
    print(row)

# Closing the database connection
conn.close()
#-----------------------------------------------------------------------------