# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 11:08:41 2019

@author: Chinmaya
"""
import pandas as pd
import glob
import os
import numpy as np
from dateutil import parser
import datetime as dt
import calendar 

#set working directory
os.chdir("D:\\GSK\\All_files")

globbed_files = glob.glob("*.csv") #creates a list of all csv files

data = [] # pd.concat takes a list of dataframes as an agrument
for csv in globbed_files:
    frame = pd.read_csv(csv)
    frame['Shipment_Name']=os.path.basename(csv)  # frame['Shipment Name'] creates a new column named filename and os.path.basename() turns a path like /a/d/c.txt into the filename c.txt
    data.append(frame)

bigframe = pd.concat(data, ignore_index=True) #dont want pandas to try an align row indexes
bigframe[['Shipment_Name','UUID','IMEI']] = bigframe['Shipment_Name'].str.split('%',expand=True)#split the column by delimiter
#bigframe[['Shipment_Name','IMEI']] = bigframe['Shipment_Name'].str.rsplit('-', 1, expand=True).rename(lambda x: f'Shipment_Name{x + 1}', axis=1)
bigframe[['IMEI','File_type']] = bigframe['IMEI'].str.rsplit('_', 1, expand=True).rename(lambda x: f'IMEI{x + 1}', axis=1)

#moves the shipment col from last to first index
ship_name = bigframe['Shipment_Name']
bigframe.drop(labels=['Shipment_Name'], axis=1, inplace=True)
bigframe.insert(0, 'Shipment_Name', ship_name)

#dedupes the dataframe
bigframe.drop_duplicates(['Shipment_Name','GPS_UTC'],keep='first')
#bigframe.drop_duplicated(['GPS_UTC'],keep='first')

#converts unix to normal time
bigframe['Arrival'] = pd.to_datetime(pd.to_numeric(bigframe['Arrival'], errors = 'coerse'),unit='ms')
bigframe['Departure'] = pd.to_datetime(pd.to_numeric(bigframe['Departure'], errors = 'coerse'),unit='ms')


#Alerts
bigframe['Stoppage']= np.where(bigframe['Alert Type']=='STOPPED', 1,
        (np.where(bigframe['Alert Type']=='STILLSTOPPED', 1,0)))       #Stoppage

bigframe['Ambient']= np.where(bigframe['Alert Type']=='AMB', 1,0)      #Ambient
bigframe['TMP_alert']  = np.where(bigframe['Alert Type']=='TEMP', 1,0)      #STemperature_alert

os.chdir("D:\GSK")
Shipment = pd.read_excel("Analytics_RawData.xlsx", usecols = ["UUID","Cold_Chain",'Actual Departure',"Origin-CFA Code","Destination-CFA Code", "Transporter Name","Origin", "O.Lat", "O.Long", "O.Add", "Destination", "D.Lat", "D.Long", "D.Add"])

result = bigframe.merge(Shipment, on= 'UUID')
#result = bigframe

result = result.drop(columns=['trackerid','flightid','flight_departure_date', 'flight_arrival_date', 'File_type'])


result= result.dropna(subset=['GPS_UTC'])
result['month'] = result['Actual Departure'].str[:3]
result['month'] = pd.DatetimeIndex(result['Actual Departure']).month
result['month'] = result['month'].apply(lambda x: calendar.month_name[x])

result.to_excel("Result.xlsx")
