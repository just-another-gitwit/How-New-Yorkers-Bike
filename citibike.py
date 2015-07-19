# download data via requests

import requests

r = requests.get('http://www.citibikenyc.com/stations/json')

# view the raw text
r.text

# view the text in json
r.json()

# view keys of data
r.json().keys()

# view file creation data
r.json()['executionTime']

# view list of stations
r.json()['stationBeanList']

# view number of docks
len(r.json()['stationBeanList'])

#run data through loop
key_list = [] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

key_list

# view first element list
r.json()['stationBeanList'][0]

# put data into dataframe using stationBeanList
from pandas.io.json import json_normalize

df = json_normalize(r.json()['stationBeanList'])

# show the dataframe
df

# import libraries for graphs
import matplotlib.pyplot as plt
import pandas as pd

# show graph for available bikes
df['availableBikes'].hist()
plt.show()

# show graph for total docks
df['totalDocks'].hist()
plt.show()

# show graph for available docks
df['availableDocks'].hist()
plt.show()

# view mean of total docks
df['totalDocks'].mean()

# view mean of total docks that are in service
condition = (df['statusValue'] == 'In Service')
df[condition]['totalDocks'].mean()

# median of total docks
df['totalDocks'].median()

# median of total docks that are in service
df[df['statusValue'] == 'In Service']['totalDocks'].median()

# create sqllite table to store data
import sqlite3 as lite

con = lite.connect('citi_bike.db')
cur = con.cursor()

with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')

# populate the table with values
#a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids] 

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")


