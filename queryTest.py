# coding=utf-8

from reading import Reading
from base import Session
from station import Station
from datetime import datetime
from time import time as timer
import fetchFunctions as Fetcher
import pandas as pd

start = timer()

# 2 - extract a session
#session = Session()

#vStationLibrary = ('00173478','00174736','00181303','00181305','00181306','00181310')

timeStartString = '2020-01-12 00:00:00'
timeEndString = '2020-01-12 01:00:00'
stationsToQuery = ['00173478']
parametersToQuery = [145,146,147,148] 

timeStart = datetime.strptime(timeStartString, '%Y-%m-%d %H:%M:%S')
timeEnd = datetime.strptime(timeEndString, '%Y-%m-%d %H:%M:%S')


print(f'Station Query made at {timer()-start}')

stations = Fetcher.fetchAllStations()
for station in stations:
	print(f'{station.station_type} Station {station.station_id} is located in {station.location} at position ({station.latitude},{station.longitude})')

print(f'Event Query made at {timer()-start}')
#events = Fetcher.fetchDayReadingsV('2020-01-12')
#response = Fetcher.fetchDayReadingsStationID('2020-01-12','00173478')
response = Fetcher.genericFetchFunction(timeStartString,timeEndString,stationsToQuery,parametersToQuery)
#df = pd.read_sql(query.statement, query.session.bind)
count = response[0]
events = response[1]
#events = Fetcher.fetchReadingsByTimeframeAndStationID(timeStartString,timeEndString,'00173478')
# events = session.query(Reading).filter(Reading.time >= timeStart, Reading.time <= timeEnd, Reading.station_id=='00173478' )
#events = Fetcher.fetchReadingByID('1385730285')

# # 4 - print movies' details

# print(type(events))
print(events)
stringEvents = events.astype(str)
events.to_csv('standard.csv')
stringEvents = stringEvents.groupby(['station_id','datetime_read'])['parameter_id','reading'].agg(' '.join).reset_index()
stringEvents.to_csv('rolledup.csv')
print(stringEvents)
# for event in events.iterrows():
#     #print(f'{station.station_id} is in {station.location}')
#     #print(type(event))
#     dataReceivedTime = timer()-start
#     print(event)a
#     #print(f'Lightning Strike at {event['datetime_read']} at {event['station_id']}')	
# print('')
#print(f'Data received at {dataReceivedTime}')

print(f'process ended at {timer()-start}')
print(f'Query has {count} items')