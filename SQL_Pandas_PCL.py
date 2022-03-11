# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 14:23:28 2022

@author: pihd
"""
# import requests
import pandas as pd
import os
# import re
import sqlite3

PWD = os.getcwd()
Account = PWD.split('\\')[2]

# URL location where Our World in Data acrhives its Covid-19 data
# https://github.com/owid/covid-19-data/tree/master/public/data
# We grab excess mortality data from above data set website for this study.
# url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/excess_mortality/excess_mortality.csv'

# path = 'C:\\Users\\DB\\Documents\\Programming\\Python\\SQL\\VSSdata\\PRECL2\\20210202_083029'
path1 = 'C:\\Users\\{}\\Documents\\Programming\\Python\\BSC\\SQL\\VSSdata\\PRECL2\\20210202_083029'.format(Account)
path2 = 'C:\\Users\\{}\\Documents\\Programming\\Python\\BSC\\SQL\\VSSdata\\PRECL2\\20210202_090510'.format(Account)

# obtain csv file name
# a= url.split('/') # split url using delimiter /
# fileName = fName = a[len(a)-1] # last split part is the file name
fileName1 = 'VSS_PRECL2_OT_083029.csv'
fileName2 = 'VSS_PRECL2_OT_090510.csv'
# The get() method sends a GET request to the specified url
# r = requests.get(url)

# Open a file, write contents in r to it and save it as csv file
# open(fName, 'wb').write(r.content)


# Pandas dataframe from reading csv file's contents
# df = pd.read_csv(fName)
df1 = pd.read_csv(path1+'/'+fileName1)
df2 = pd.read_csv(path2+'/'+fileName2)

df3 = pd.concat([df1, df2])
# df3.reset_index(inplace = True)


# df1.drop(df1.columns[0], axis=1, inplace=True)
# df2.drop(df2.columns[0], axis=1, inplace=True)
# df3.drop(df3.columns[0], axis=1, inplace=True)

# df3.reset_index()
# df1=df1.style.hide_index()
# df2=df2.style.hide_index()
# df3=df3.style.hide_index()

# Create sqlite3 table name
tableName = fileName1.split('OT_')[0]+'OT'
# tableName2 = fileName2.split('.csv')[0]


#Creating a Connection between sqlite3 database using table name from above
conn = sqlite3.connect(':memory:')

# Write records stored in a DataFrame to a SQL database
df1.to_sql(tableName,             # Name of the sql table
         conn,                 # sqlite.Connection or sqlalchemy.engine.Engine
         # index=True,
         if_exists='replace')  # If the table already exists, {‘fail’, ‘replace’, ‘append’}, default ‘fail’

df2.to_sql(tableName,             # Name of the sql table
         conn,                 # sqlite.Connection or sqlalchemy.engine.Engine
         # index=True,
         index_label='index',
         if_exists='append')  # If the table already exists, {‘fail’, ‘replace’, ‘append’}, default ‘fail’

# Read a Pandas data frame from a sqlite3 table
# f_out = pd.read_sql('''SELECT * FROM %s'''%tableName,
#                     conn,
#                     index_col='index')

f_out = pd.read_sql('''SELECT * FROM %s'''%tableName, conn)
f_out.reset_index()
f_out.drop(f_out.columns[0], axis=1, inplace=True)

mycursor = conn.cursor()

# mycursor.execute("ALTER TABLE %s AUTO_INCREMENT = 1" %tableName)
# mycursor.execute("df.reset_index(drop=True)")
# mycursor.execute("SELECT * from %s ORDER BY 'index' DESC LIMIT 1" %tableName)
# mycursor.execute("DROP INDEX index")
mycursor.execute("SELECT * from %s ORDER BY 'index'" %tableName)
# mycursor.execute("SELECT * FROM %s"%tableName)
 
myresult = mycursor.fetchall()
# print('fetchone:')
print(myresult)
# newRow1 = myresult[0]
# mycursor.execute("INSERT INTO :name VALUES :row", {'name':tableName, 'row':newRow1} )


conn.close()

# print('info:')
# print(f_out.info())

# print('head:')
# # print(f_out.head())
# print(f_out)


cNames = df1.columns.values