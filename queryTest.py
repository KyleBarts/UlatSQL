# coding=utf-8

from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import fetchFunctions as Fetcher
import pandas as pd
import processFunctions as Processer
import requests

start = timer()

station_details = pd.read_csv('StationDetails2.csv', dtype=object, names=['Station Name','Serial'])
station_details = station_details.astype(str)
station_dict = {}
for index, row in station_details.iterrows():
    station_dict[row['Serial']]=row['Station Name']


#Edit this if you wanna change date and time THIS IS IN PST
timeStartString = '2020-05-27 18:30:00'
timeEndString = '2020-05-27 21:29:59'
#timeEndString = '2020-11-01 23:59:59'

print(station_details)
pStationIDList = station_details['Serial'].tolist()
# count=0
# for item in pStationIDList:
# 	pStationIDList[count] = '00'+item
# 	count=count+1
print(pStationIDList)
reading_details = pd.read_csv('reading_details.csv')
print(reading_details)
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

# def join_station_name_to_row(row):
# 	current_serial = row['station_id']
# 	current_details = station_details[station_details['Serial']==current_serial]
# 	current_station_name = current_details['Station Name'].iloc[0]
# 	#current_station_name = current_details['Station Name']
# 	print(current_station_name)
# 	row['Station Name'] = current_station_name
# 	#current_station_name = 'hello'
# 	return current_station_name

def map_reading_names(df,reading_details):
	
	select_reading = reading_details.loc[reading_details['parameter_id'] == df.parameter_id]
	#print(select_reading)
	parameter_key = select_reading['parameter_key'].values[0]
	#paired_up = (f'{parameter_key}',df.reading)
	paired_up = f'{parameter_key}:{df.reading}'
	return paired_up



def v_lightning_event_generic(timeStart,timeEnd):
	vStationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
	
	parametersToQuery = [146] #TPP
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,vStationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	#final_dataframe = final_dataframe.groupby(['datetime_read'])['station_id'].agg(' '.join).reset_index()

	return (final_dataframe)

def v_lightning_event_filtered(timeStart,timeEnd):
	vStationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
	
	parametersToQuery = [145,146, 147, 148, 149, 150,151,152] #TPP
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,vStationsToQuery,parametersToQuery)
	
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe

	print(f'response received at {timer()-start}')
	print(events)
	#events = events.groupby(['datetime_read','station_id'])
	#events['new_reading'] = events.apply(lambda x: map_reading_names(x,reading_details),axis = 1)
	print(events)
	events = events.groupby(['datetime_read','station_id'])['reading'].agg([('readings',lambda x:  str(x.values)), 'count',('|TPP-TPN|',lambda x: abs( int(x.values[1][4:])-int(x.values[3][4:])))]).reset_index()

	print(events)
	filtered_events = events[(events['|TPP-TPN|']>=2) & (events['count']==8)].reset_index(drop=1)
	print(filtered_events)
	filtered_events = filtered_events.astype(str)
	filtered_events['readings'] = filtered_events['readings'].apply(lambda x: x.strip('[]').replace('\'',''))
	filtered_events[['TPS','TPP','TPZ','TPN','APP','APN','APF','APC']] = filtered_events['readings'].str.split(' ',expand=True)
	filtered_events = filtered_events.drop(columns=['readings','count'])
	filtered_events = filtered_events.groupby('datetime_read').agg(lambda x: list(x))
	print(filtered_events)
	filtered_events = filtered_events[ filtered_events['station_id'].map(len)>2 ]
	final_dataframe = filtered_events.astype(str) #convert everything to string for pandas compatibility
	#final_dataframe = final_dataframe.groupby(['datetime_read'])['station_id'].agg(' '.join).reset_index()

	return (final_dataframe)


def v_lightning_event_filtered_new(timeStart,timeEnd):
    vStationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']

    parametersToQuery = [146] 
    response = Fetcher.newLightningFetchFunction(timeStart,timeEnd,vStationsToQuery,parametersToQuery)
    count = response[0] #Integer representing # of readings in query
    events = response[1] #Query object containing raw readings to be processed into dataframe
    print(f'response received at {timer()-start}')
    print(events)
    
    events['|TPP-TPN|'] = abs(events['tpp'] - events['tpn'])

    events = events.drop(columns=['id'])

    #Filter out all events where |TPP-TP| < 0.2
    filtered_events = events[events['|TPP-TPN|']>=0.2].reset_index(drop=1)
    
    print(filtered_events)
    #Uncomment this if unfiltered data is needed
    #filtered_events = events

    
    #Group events by the second
    grouped_events = filtered_events.groupby('datetime').agg(lambda x: list(x))
    print(grouped_events)
    print('Grouped above')
    
    #Filter out all groups with less than 3 events
    grouped_atleast3_events = grouped_events[grouped_events['station_id'].map(len)>2]
    #grouped_atleast3_events = grouped_events
    print(grouped_atleast3_events)
    print('Grouped with 3 events above')
    #Count number of distinct stations per group
    grouped_atleast3_events['Distinct Stations'] = grouped_atleast3_events['station_id'].apply(lambda x: len(set(x)))

    #Filter out all groups with less than 3 unique stations
    grouped_atleast3_events = grouped_atleast3_events[grouped_atleast3_events['Distinct Stations'] >2]

    #This is for counting total number of events
    grouped_atleast3_events['Event Count'] = grouped_atleast3_events['station_id'].apply(lambda x: len(x))

    #This is for mapping all events to their station names    
    grouped_atleast3_events['Station Names'] = grouped_atleast3_events['station_id'].apply(lambda x: list(pd.Series(x).map(station_dict)))

    ordered_columns = ["Distinct Stations","Event Count","station_id","Station Names","tps","tpp","tpz","tpn","app","apn","apf","apc","|TPP-TPN|"]


    final_dataframe = grouped_atleast3_events
    
    final_dataframe = final_dataframe.reindex(columns=ordered_columns)
    return final_dataframe

def v_lightning_event_monthly_count(timeStart,timeEnd):
	VstationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
	#VstationsToQuery = ['00174727']
	parametersToQuery = [145]
	response = Fetcher.countFetchFunction(timeStart,timeEnd,VstationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	#final_dataframe = Processer.per_min_event_count_no_events(final_dataframe)
	#final_dataframe = Processer.new_per_hour_event_count_no_events(final_dataframe)
	final_dataframe.insert(1,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	return (final_dataframe)


def v_lightning_event_minute_count(timeStart,timeEnd):
	VstationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
	
	#VstationsToQuery = ['00174727']
	parametersToQuery = [145]
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,VstationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	final_dataframe = Processer.per_min_event_count_no_events(final_dataframe)
	#final_dataframe = Processer.new_per_hour_event_count_no_events(final_dataframe)
	final_dataframe.insert(1,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	return (final_dataframe)

def v_lightning_event_matcher(timeStart,timeEnd):
	VstationsToQuery = ['00173478','00174736','00181303','00181305','00181306','00181310']
	#VstationsToQuery = ['00173478','00174736','00181305','00181310']
	#VstationsToQuery = ['00174727']
	parametersToQuery = [146]
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,VstationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	#final_dataframe = Processer.per_min_event_count_no_events(final_dataframe)
	#final_dataframe = Processer.new_per_hour_event_count_no_events(final_dataframe)
	final_dataframe = final_dataframe.sort_values(by=['datetime_read'])
	final_dataframe.insert(1,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	return (final_dataframe)

def p_lightning_event_minute_count(timeStart,timeEnd):
	pStationsToQuery = pStationIDList
	#pStationsToQuery = ['00174727']
	parametersToQuery = [138]
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,pStationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	final_dataframe = Processer.per_min_event_count_no_events(final_dataframe)
	#final_dataframe = Processer.new_per_hour_event_count_no_events(final_dataframe)
	final_dataframe.insert(1,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	return (final_dataframe)

def p_lightning_event_generic(timeStart,timeEnd):
	pStationsToQuery = pStationIDList
	parametersToQuery = [138]
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,pStationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	#final_dataframe = final_dataframe.groupby(['datetime_read'])['station_id'].agg(' '.join).reset_index()

	return (final_dataframe)

def aws_generic(timeStart,timeEnd):
	pStationsToQuery = pStationIDList 
	pStationsToQuery = ['00181305']
	#aws_parameters = [2,5,6,9,12,48,125,126,127,128,129,130,131,132,133,134,135,136,137]
	aws_parameters = [133]
	#133 is Max wind speed in 1 min
	#127 is Effective rain amount
	#5 is Temperature

	response = Fetcher.genericFetchFunction(timeStart,timeEnd,pStationsToQuery,[133])
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	#final_dataframe = final_dataframe.groupby(['datetime_read'])['station_id'].agg(' '.join).reset_index()
	final_dataframe.insert(1,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	return (final_dataframe)

def p_lightning_event_group_by_time(timeStart,timeEnd):
	pStationsToQuery = pStationIDList
	parametersToQuery = [138]
	response = Fetcher.genericFetchFunction(timeStart,timeEnd,pStationsToQuery,parametersToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	
	final_dataframe = events.astype(str) #convert everything to string for pandas compatibility
	final_dataframe = final_dataframe.groupby(['datetime_read'])['station_id'].agg(' '.join).reset_index()

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
	#final_dataframe = final_dataframe.reset_index(drop=True)

	final_dataframe.insert(0,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	final_dataframe.insert(1,'LON', '')
	final_dataframe['LON'] = final_dataframe.apply(Processer.return_lon_to_row,args=(stations,), axis=1)
	final_dataframe.insert(1,'LAT', '')
	final_dataframe['LAT'] = final_dataframe.apply(Processer.return_lat_to_row,args=(stations,), axis=1)

	
	final_dataframe = final_dataframe.rename(columns={'station_id': 'Station Code'})
	print(final_dataframe)
	return final_dataframe

def qgis_format_generator_pressure(timeStart,timeEnd):
	pStationsToQuery = pStationIDList
	rainParameterToQuery = [127]
	tempParameterToQuery = [6]
	response = Fetcher.generic_fetch_function_15mins(timeStart,timeEnd,pStationsToQuery,tempParameterToQuery)
	count = response[0] #Integer representing # of readings in query
	events = response[1] #Query object containing raw readings to be processed into dataframe
	print(f'response received at {timer()-start}')
	print(events)
	stringEvents  = events.astype(str)
	stringEvents['datetime_read'] = stringEvents['datetime_read'].apply(Fetcher.convertStringToPhilippineTime)
	print(stringEvents)
	final_dataframe = stringEvents
	stringEvents['reading'] = stringEvents['reading'].apply(Processer.convert_pressure)
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

	final_dataframe.insert(0,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	final_dataframe.insert(1,'LON', '')
	final_dataframe['LON'] = final_dataframe.apply(Processer.return_lon_to_row,args=(stations,), axis=1)
	final_dataframe.insert(1,'LAT', '')
	final_dataframe['LAT'] = final_dataframe.apply(Processer.return_lat_to_row,args=(stations,), axis=1)
	
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	final_dataframe = final_dataframe.rename(columns={'station_id': 'Station Code'})
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

	final_dataframe['time'] =  final_dataframe['datetime_read'].apply(lambda x: x[11:16]) #isolate hour to second
	final_dataframe = final_dataframe.sort_values(by=['time'])
	final_dataframe = final_dataframe.groupby(['station_id'])['time','reading'].agg(' '.join).reset_index() #roll all station's readings into one row
	final_dataframe['time'] = final_dataframe['time'].apply(lambda x: x.split(' ')) #convert string of event timestamps into a list
	final_dataframe['reading'] = final_dataframe['reading'].apply(lambda x: x.split(' ')) #convert string of event readings into list
	print(final_dataframe)
	for time_stamp in time_columns:
		final_dataframe[time_stamp] = final_dataframe.apply(Processer.column_parser,args=(time_stamp,),axis=1)

	final_dataframe = final_dataframe.drop(columns=['time','reading'])
	final_dataframe.insert(1,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	print(final_dataframe)

	return final_dataframe

def qgis_format_generator_lightning_count(timeStart,timeEnd):
	df = p_lightning_event_generic(timeStart,timeEnd)
	df = Processer.count_lightning_events_15mins(df)
	df = df.rename(columns={'count':'reading'})
	df = df.astype(str)
	time_columns = pd.Series(Fetcher.generate_time_increments(15, datetime.strptime(timeStartString, '%Y-%m-%d %H:%M:%S'), datetime.strptime(timeEndString, '%Y-%m-%d %H:%M:%S'))).astype(str)
	time_columns = time_columns.apply(lambda x: x[11:16])
	print(time_columns)
	#For 15 min temps

	final_dataframe = df.sort_values(by=['datetime_read'])


	final_dataframe['time'] =  final_dataframe['datetime_read'].apply(lambda x: x[11:16]) #isolate hour to second
	# final_dataframe = final_dataframe.sort_values(by=['time'])
	final_dataframe = final_dataframe.groupby(['station_id'])['time','reading'].agg(' '.join).reset_index() #roll all station's readings into one row
	final_dataframe['time'] = final_dataframe['time'].apply(lambda x: x.split(' ')) #convert string of event timestamps into a list
	final_dataframe['reading'] = final_dataframe['reading'].apply(lambda x: x.split(' ')) #convert string of event readings into list
	# print(final_dataframe)
	for time_stamp in time_columns:
		final_dataframe[time_stamp] = final_dataframe.apply(Processer.column_parser,args=(time_stamp,),axis=1)

	final_dataframe = final_dataframe.drop(columns=['time','reading'])
	final_dataframe.insert(1,'Station Name', '')
	final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
	#print(final_dataframe)

	return final_dataframe


#vStationLibrary = ('00173478','00174736','00181303','00181305','00181306','00181310')
 


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
print(stations)
# for station in stations:
# 	print(f'{station.station_type} Station {station.station_id} is located in {station.location} at position ({station.latitude},{station.longitude})')

print(f'Event Query made at {timer()-start}')

#V POTEKA TESTING 
#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,VstationsToQuery,parametersToQuery)

#P POTEKA PLATE TESTING
#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,pStationsToQuery,plateParameterToQuery)

#P POTEKA TEMP TESTING

#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,pStationsToQuery,tempParameterToQuery)

#AWS Testing
#response = Fetcher.genericFetchFunction(timeStartString,timeEndString,"00174722",aws_parameters)

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
#For P Poteka Generic Event Fetch
#final_dataframe = p_lightning_event_generic(timeStartString, timeEndString)
#final_dataframe = Processer.count_lightning_events_15mins(final_dataframe)

#For P Poteka Lightning Event OpenCV Map
#final_dataframe = p_lightning_event_group_by_time(timeStartString, timeEndString)

#For V Poteka Lightning Event Count Per Minute
#final_dataframe = v_lightning_event_matcher(timeStartString, timeEndString)

#For V Poteka Lightning Event Count Per Minute
#final_dataframe = v_lightning_event_minute_count(timeStartString, timeEndString)

#For V Poteka Monthly Lightning Event Count
#final_dataframe = v_lightning_event_monthly_count(timeStartString, timeEndString)

#For V Poteka Generic Event Fetch TPP
# final_dataframe = v_lightning_event_generic(timeStartString, timeEndString)
# final_dataframe = Processer.append_microseconds_to_datetime(final_dataframe)
# final_dataframe = Processer.append_time_diff(final_dataframe)

#V Poteka IDL Filtering
# final_dataframe = v_lightning_event_filtered(timeStartString, timeEndString)


# final_dataframe = Processer.append_microseconds_to_datetime(final_dataframe)
# final_dataframe = Processer.append_time_diff(final_dataframe)

#For P Poteka Lightning Event Count Per Minute
#final_dataframe = p_lightning_event_minute_count(timeStartString, timeEndString)

#For AWS Stuff
#final_dataframe = aws_generic(timeStartString, timeEndString)

#For Generating QGIS Format
#final_dataframe = qgis_format_generator_temp(timeStartString, timeEndString)

#final_dataframe = qgis_format_generator_pressure(timeStartString, timeEndString)

#For Generating QGIS Lightning Event
#final_dataframe = qgis_format_generator_lightning_count(timeStartString, timeEndString)

#Universal adjustments
#final_dataframe.insert(1,'Station Name', '')
#final_dataframe['Station Name'] = final_dataframe.apply(Processer.join_station_name_to_row,args=(station_details,), axis=1)
#final_dataframe.to_csv('may_15_ERA.csv')
#final_dataframe = final_dataframe.rename(columns={"datetime_read":"Date & Time (PST)", "count":"Lightning Events per Minute"})



#Saving to csv for vizualization
#final_dataframe.to_csv('./outputs/may_15_pressure.csv', index=False)
#print(final_dataframe)

# import requests

# payload = {'start_date': timeStartString,
# 			'end_date':timeEndString,
# 			'flash_type':0
# 		}
# pagasa_response = requests.get(
#     'http://192.168.6.179:8080/earthnetworks',
#     params=payload,
# )

# pagasa_json = pagasa_response.content
# pagasa_df = pd.read_json(pagasa_json,orient='records')
# print(pagasa_df)


#timestamps = final_dataframe.index.astype(str).tolist()
#print(timestamps)

#pagasa_data = pd.read_csv('./pagasa_data/outputs/lightning_data_05_2020.csv', dtype=object)
#pagasa_data_range = pagasa_data

#pagasa_data = pagasa_data[pagasa_data.lightning_time.isin(timestamps)]

# pagasa_data_range['lightning_time'] = pd.to_datetime(pagasa_data_range['lightning_time'])
# date_mask = (pagasa_data_range['lightning_time'] > timeStartString) & (pagasa_data_range['lightning_time'] <= timeEndString)
# pagasa_data_range = pagasa_data_range.loc[date_mask]
# print(pagasa_data.astype(str))
# print(f'pagasa_data has {len(pagasa_data.index)} lightning events')
# print(f'poteka_data has {len(final_dataframe.index)} lightning events')
# pagasa_data = pagasa_data[pagasa_data['flash_type']=='1']
# print(pagasa_data.astype(str))
# print(f'pagasa_data has {len(pagasa_data.index)} lightning events')
# pagasa_data.to_csv('./outputs/Oct30_Pasig_Pagasa_Lightning.csv')
# pagasa_data_range.to_csv('./outputs/Oct30_Pasig_Pagasa_Range_Lightning.csv')


print(f'process ended at {timer()-start}')
#print(f'Query has {count} items')
