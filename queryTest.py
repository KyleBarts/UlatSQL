# coding=utf-8

from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import fetchFunctions as Fetcher
import pandas as pd

start = timer()

def process_vlf_dataframe(df):
	"""
	This function takes in a dataframe from a query with STRICTLY THE VLF PARAMETER LIST [145,146,147,147]
	and outputs a dataframe with properly labelled columns
	"""
	df[['TPS','TPP','TPZ','TPN']] = df.reading.str.split(expand=True) 
	df= df.drop(columns=['parameter_id','reading'])
	df = df.sort_values(by=['datetime_read'])
	return df

def process_temp_dataframe(df,parameter_names):
	"""
	This function takes in a dataframe from a query with STRICTLY TEMP PARAMETERS [5]
	and outputs a dataframe with properly labelled columns
	"""
	df[parameter_names] = df.reading.str.split(expand=True)  
	df= df.drop(columns=['parameter_id','reading'])
	df = df.sort_values(by=['datetime_read'])
	return df




#vStationLibrary = ('00173478','00174736','00181303','00181305','00181306','00181310')

timeStartString = '2020-05-12 00:00:00' #This is in PST
timeEndString = '2020-05-12 01:00:00' 
stationsToQuery = ['00173478']

#Below are parameters for V-Poteka Event Testing 
# Uncomment lines labelled V POTEKA TESTING to try
# NOTE if you enable V TESTING lines make sure to disable P TESTING lines
VstationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
parametersToQuery = [145,146,147,148]

#Below are parameters for P-Poteka Temp Testing
pStationsToQuery = ['00174722']  #just keep adding to this list if you want to add more stations 
tempParameterToQuery = [5]
pPotekaParameterName = ['Temp']

"""
You can add more parameters to the list above by simply adding more values to the list
as such if you want to include pressure 
REFER TO reading_details.csv for full list of parameters

tempParameterToQuery = [5,6]
pPotekaParameterName = ['Temp',"Pressure"]
"""


print(f'Station Query made at {timer()-start}')
#This is simply for testing if the code can connect to the server by fetching the list of stations
stations = Fetcher.fetchAllStations()
for station in stations:
	print(f'{station.station_type} Station {station.station_id} is located in {station.location} at position ({station.latitude},{station.longitude})')

print(f'Event Query made at {timer()-start}')

#V POTEKA TESTING 
#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,VstationsToQuery,parametersToQuery)

#P POTEKA TEMP TESTING
response = Fetcher.genericFetchFunction(timeStartString,timeEndString,pStationsToQuery,tempParameterToQuery)

count = response[0] #Integer representing # of readings in query
events = response[1] #Query object containing raw readings to be processed into dataframe


print(events)
stringEvents = events.astype(str) #convert everything to string for pandas compatibility
events.to_csv('standard.csv')

"""
This line below combines all entries which include the same datetime_read AND station_id
it essentally consolidates all parameters back into the original event that the station sent
"""
stringEvents = stringEvents.groupby(['station_id','datetime_read'])['parameter_id','reading'].agg(' '.join).reset_index()
stringEvents.to_csv('rolledup.csv')
print(stringEvents)

#V POTEKA TESTING
#testing = process_vlf_dataframe(stringEvents)

#P POTEKA TEMP TESTING
final_dataframe = process_temp_dataframe(stringEvents,pPotekaParameterName)

final_dataframe.to_csv('testing.csv')
print(final_dataframe)


print(f'process ended at {timer()-start}')
print(f'Query has {count} items')