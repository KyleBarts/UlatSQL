# coding=utf-8

from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import fetchFunctions as Fetcher
import pandas as pd
import processFunctions as Processer

start = timer()

station_details = pd.read_csv('StationDetails2.csv', dtype=object, names=['Station Name','Serial'])
station_details = station_details.astype(str)
print(station_details)
pStationIDList = station_details['Serial'].tolist()
# count=0
# for item in pStationIDList:
# 	pStationIDList[count] = '00'+item
# 	count=count+1
print(pStationIDList)

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

def join_station_name_to_row(row):
	current_serial = row['station_id']
	current_details = station_details[station_details['Serial']==current_serial]
	current_station_name = current_details['Station Name'].iloc[0]
	#current_station_name = current_details['Station Name']
	print(current_station_name)
	row['Station Name'] = current_station_name
	#current_station_name = 'hello'
	return current_station_name


def v_lightning_event_generic(timeStart,timeEnd):
	VstationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
	parametersToQuery = [145]
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,VstationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	final_dataframe = Processer.per_min_event_count_no_events(final_dataframe)
	#final_dataframe = Processer.new_per_hour_event_count_no_events(final_dataframe)
	return (final_dataframe)

def qgis_format_generator_temp(timeStart,timeEnd):
	pStationsToQuery = pStationIDList
	rainParameterToQuery = [127]
	tempParameterToQuery = [5]
	response = Fetcher.generic_fetch_function_15mins(timeStart,timeEnd,pStationsToQuery,tempParameterToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	stringEvents  = events.astype(str)
	stringEvents['datetime_read'] = stringEvents['datetime_read'].apply(Fetcher.convertStringToPhilippineTime)
	print(stringEvents)
	final_dataframe = stringEvents
	stringEvents['reading'] = stringEvents['reading'].apply(Processer.convert_temp)
	time_columns = pd.Series(Fetcher.generate_time_increments(15, datetime.strptime(timeStartString, '%Y-%m-%d %H:%M:%S'), datetime.strptime(timeEndString, '%Y-%m-%d %H:%M:%S'))).astype(str)
	time_columns = time_columns.apply(lambda x: x[11:16])
	print(time_columns)
	#For 15 min temps

	final_dataframe = final_dataframe.sort_values(by=['datetime_read'])
	#final_dataframe['time'] = (final_dataframe['datetime_read']+'')[:]
	final_dataframe['time'] =  final_dataframe['datetime_read'].apply(lambda x: x[11:16])
	final_dataframe = final_dataframe.sort_values(by=['time'])
	final_dataframe = final_dataframe.groupby(['station_id'])['time','reading'].agg(' '.join).reset_index()
	final_dataframe['time'] = final_dataframe['time'].apply(lambda x: x.split(' '))
	final_dataframe['reading'] = final_dataframe['reading'].apply(lambda x: x.split(' '))
	print(final_dataframe)
	for time_stamp in time_columns:
		final_dataframe[time_stamp] = final_dataframe.apply(Processer.column_parser,args=(time_stamp,),axis=1)

	final_dataframe = final_dataframe.drop(columns=['time','reading'])
	print(final_dataframe)
	return final_dataframe

def qgis_format_generator_rain(timeStart,timeEnd):
	pStationsToQuery = pStationIDList
	rainParameterToQuery = [127]
	response = Fetcher.generic_fetch_function_15mins(timeStart,timeEnd,pStationsToQuery,rainParameterToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	stringEvents  = events.astype(str)
	stringEvents['datetime_read'] = stringEvents['datetime_read'].apply(Fetcher.convertStringToPhilippineTime)
	print(stringEvents)
	final_dataframe = stringEvents
	time_columns = pd.Series(Fetcher.generate_time_increments(15, datetime.strptime(timeStartString, '%Y-%m-%d %H:%M:%S'), datetime.strptime(timeEndString, '%Y-%m-%d %H:%M:%S'))).astype(str)
	time_columns = time_columns.apply(lambda x: x[11:16])
	print(time_columns)
	#For 15 min temps

	final_dataframe = final_dataframe.sort_values(by=['datetime_read'])

	final_dataframe['time'] =  final_dataframe['datetime_read'].apply(lambda x: x[11:16])
	final_dataframe = final_dataframe.sort_values(by=['time'])
	final_dataframe = final_dataframe.groupby(['station_id'])['time','reading'].agg(' '.join).reset_index()
	final_dataframe['time'] = final_dataframe['time'].apply(lambda x: x.split(' '))
	final_dataframe['reading'] = final_dataframe['reading'].apply(lambda x: x.split(' '))
	print(final_dataframe)
	for time_stamp in time_columns:
		final_dataframe[time_stamp] = final_dataframe.apply(Processer.column_parser,args=(time_stamp,),axis=1)

	final_dataframe = final_dataframe.drop(columns=['time','reading'])
	print(final_dataframe)
	return final_dataframe


#vStationLibrary = ('00173478','00174736','00181303','00181305','00181306','00181310')

#Edit this if you wanna change date and time THIS IS IN PST
timeStartString = '2020-05-06 00:00:00'
timeEndString = '2020-05-06 23:59:59' 


stationsToQuery = ['00173478'] #This is for v poteka testing UPLB

#Below are parameters for V-Poteka Event Testing 
# Uncomment lines labelled V POTEKA TESTING to try
# NOTE if you enable V TESTING lines make sure to disable P TESTING lines
VstationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
pStationsToQuery = pStationIDList
#parametersToQuery = [145,146,147,148]
parametersToQuery = [145]

#Below are parameters for P-Poteka Temp Testing
#pStationsToQuery = ['00174722','00181293','00181285']  #just keep adding to this list if you want to add more stations 
#pStationsToQuery = ['00174722']
#pStationsToQuery = ['00181284']
plateParameterToQuery = [138]

tempParameterToQuery = [5]
pPotekaParameterName = ['Temp']

rainParameterToQuery = [127]

"""
You can add more parameters to the list above by simply adding more values to the list
as such if you want to include pressure 
REFER TO reading_details.csv for full list of parameters

tempParameterToQuery = [5,6]
pPotekaParameterName = ['Temp','Pressure']
"""


print(f'Station Query made at {timer()-start}')
#This is simply for testing if the code can connect to the server by fetching the list of stations
stations = Fetcher.fetchAllStations()
for station in stations:
	print(f'{station.station_type} Station {station.station_id} is located in {station.location} at position ({station.latitude},{station.longitude})')

print(f'Event Query made at {timer()-start}')

#V POTEKA TESTING 
#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,VstationsToQuery,parametersToQuery)

#P POTEKA PLATE TESTING
#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,pStationsToQuery,plateParameterToQuery)

#P POTEKA TEMP TESTING

#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,pStationsToQuery,tempParameterToQuery)

#P POTEKA LATEST TEMPS
#response = Fetcher.genericLatestWeatherReadingAllP(pStationsToQuery,tempParameterToQuery)

#P POTEKA RAIN TESTING
#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,pStationsToQuery,rainParameterToQuery)

"""


count = response[0] #Integer representing # of readings in query
events = response[1] #Query object containing raw readings to be processed into dataframe

print(f'response received at {timer()-start}')
print(events)
stringEvents = events.astype(str) #convert everything to string for pandas compatibility
events.to_csv('standard.csv')

"""
#This line below combines all entries which include the same datetime_read AND station_id
#it essentally consolidates all parameters back into the original event that the station sent
"""
#This is for fetching an entire lightning event string
# stringEvents = stringEvents.groupby(['station_id','datetime_read'])['parameter_id','reading'].agg(' '.join).reset_index()
# stringEvents.to_csv('rolledup.csv')


stringEvents['datetime_read'] = stringEvents['datetime_read'].apply(Fetcher.convertStringToPhilippineTime)
print(stringEvents)

#V POTEKA TESTING
#final_dataframe = process_vlf_dataframe(stringEvents)
final_dataframe = stringEvents
#final_dataframe = final_dataframe.drop(columns=['parameter_id','reading'])
#P POTEKA TEMP TESTING
#final_dataframe = process_temp_dataframe(stringEvents,pPotekaParameterName)
#final_dataframe = Processer.per_day_event_count_no_events(final_dataframe)


#For Per Min Temp Readings
#final_dataframe = Processer.per_min_temp(final_dataframe)

#For Per Min Rain Readings
#final_dataframe = Processer.per_min_rain(final_dataframe)


#For 15 min temps
"""
#For V Poteka Lightning Event Count
#final_dataframe = v_lightning_event_generic(timeStartString, timeEndString)

#For Generating QGIS Format
final_dataframe = qgis_format_generator_temp(timeStartString, timeEndString)


#Universal adjustments
final_dataframe.insert(1,'Station Name', '')
final_dataframe['Station Name'] = final_dataframe.apply(join_station_name_to_row, axis=1)
#final_dataframe.to_csv('may_15_ERA.csv')
#final_dataframe = final_dataframe.rename(columns={"datetime_read":"Date & Time (PST)"})

#Saving to csv for vizualization
final_dataframe.to_csv('May_6_temp.csv')
print(final_dataframe)




print(f'process ended at {timer()-start}')
#print(f'Query has {count} items')
