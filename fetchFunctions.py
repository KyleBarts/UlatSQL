from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
from sqlalchemy import func
import time
import pandas as pd


session = Session()

vStationLibrary = ('00173478','00174736','00181303','00181305','00181306','00181310')


def convertToPhilippineTime(dateTime):
	return dateTime + timedelta(hours=8)

def get_count(q):
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    count = q.session.execute(count_q).scalar()
    return count

#Fetch all Stations
def fetchAllStations():
	stations = session.query(Station).all()
	return stations



#Fetch all V POTEKA Stations
def fetchVStations():
	stations = session.query(Station).filter(Station.station_id.in_(vStationLibrary))
	return stations



#Fetch all P POTEKA Stations
def fetchPStations():
	stations = session.query(Station).filter(Station.station_id.notin_(vStationLibrary))
	return stations

def genericFetchFunction(datetimeStart, datetimeEnd,stationList,parameterList):
	""" The most customizable, generic fetch function keeping relative simpliciyu
	INPUTS
	string datetimeStart: starting date and time in string with format "YYYY-MM-DD HH:MM:SS"
	string datetimeEnd: starting date and time in string with same format
	list stationList: list of strings signifying station ID number example ['00173478','00174736']
	list parameterList: list if ints signifying parameters to be searched example [145,146,147]
	OUTPUTS
	integer count: number of readings returned
	events: pandas dataframe object of actual readings returned NOTE these are readings and not lightning events. 
	refer to parameter list to know which readings refer to lightning events
	"""
	timeStart= datetime.strptime(datetimeStart, '%Y-%m-%d %H:%M:%S')
	timeEnd = datetime.strptime(datetimeEnd , '%Y-%m-%d %H:%M:%S')
	timeStartPST = convertToPhilippineTime(timeStart)
	timeEndPST = convertToPhilippineTime(timeEnd)
	#events = session.query(Reading).filter(Reading.time >= timeStartPST, Reading.time <= timeEndPST, Reading.station_id.in_(stationList), Reading.parameter_id.in_(parameterList))
	events = pd.read_sql(session.query(Reading).filter(Reading.time >= timeStartPST, Reading.time <= timeEndPST, Reading.station_id.in_(stationList), Reading.parameter_id.in_(parameterList)).statement,session.bind) 
	#count = get_count(events)
	count = events.size
	return count,events

#Fetch all Readings in a day for a single station given ID number
#Input dateString of this format: 'YYYY-MM-DD' 
#Input chosen_id corresponding to station id 
def fetchDayReadingsStationID(dateString, chosen_id):
	# date = datetime.strptime(dateString, '%Y-%m-%d')
	# day =  date.date
	# dayAfter = date.date + timedelta(days=1)
	# print(f'requesting for {day.date} and {dayAfter.date}')
	timeStart= datetime.strptime(dateString+' 00:00:00', '%Y-%m-%d %H:%M:%S')
#	timeEnd = datetime.strptime(dateString+' 23:59:59' , '%Y-%m-%d %H:%M:%S')
	timeEnd = datetime.strptime(dateString+' 01:59:59' , '%Y-%m-%d %H:%M:%S')
	timeStartPST = convertToPhilippineTime(timeStart)
	timeEndPST = convertToPhilippineTime(timeEnd)
	print(f'requesting for {timeStartPST} till {timeEndPST}')
	events = session.query(Reading).filter(Reading.time >= timeStartPST, Reading.time <= timeEndPST, Reading.station_id==chosen_id, Reading.parameter_id==145)
	count = get_count(events)
	return count,events

#Fetch all V POTEKA Readings in a day
#Input dateString of this format: 'YYYY-MM-DD' 
#for example if you want data of January 12, 2020, input '2020-01-12' 
def fetchDayReadingsV(dateString):
	timeStart= datetime.strptime(dateString+' 00:00:00', '%Y-%m-%d %H:%M:%S')
	timeEnd = datetime.strptime(dateString+' 23:59:59' , '%Y-%m-%d %H:%M:%S')
	events = session.query(Reading).filter(Reading.time >= timeStart, Reading.time <= timeEnd, Reading.station_id.in_(vStationLibrary))
	return events



#Fetch all P POTEKA Readings in a day
#Input dateString of this format: 'YYYY-MM-DD' 
#for example if you want data of January 12, 2020, input '2020-01-12'
def fetchDayReadingsP(dateString):
	timeStart= datetime.strptime(dateString+' 00:00:00', '%Y-%m-%d %H:%M:%S')
	timeEnd = datetime.strptime(dateString+' 23:59:59' , '%Y-%m-%d %H:%M:%S')
	events = session.query(Reading).filter(Reading.time >= timeStart, Reading.time <= timeEnd, Reading.station_id.notin_(vStationLibrary))
	return events 





#Fetch all Readings in a day for a single station given ID number
#Input dateString of this format: 'YYYY-MM-DD' 
#Input chosen_id corresponding to station id 
def fetchDayReadingsStationID(dateString, chosen_id):
	# date = datetime.strptime(dateString, '%Y-%m-%d')
	# day =  date.date
	# dayAfter = date.date + timedelta(days=1)
	# print(f'requesting for {day.date} and {dayAfter.date}')
	timeStart= datetime.strptime(dateString+' 00:00:00', '%Y-%m-%d %H:%M:%S')
#	timeEnd = datetime.strptime(dateString+' 23:59:59' , '%Y-%m-%d %H:%M:%S')
	timeEnd = datetime.strptime(dateString+' 01:59:59' , '%Y-%m-%d %H:%M:%S')
	timeStartPST = convertToPhilippineTime(timeStart)
	timeEndPST = convertToPhilippineTime(timeEnd)
	print(f'requesting for {timeStartPST} till {timeEndPST}')
	events = session.query(Reading).filter(Reading.time >= timeStartPST, Reading.time <= timeEndPST, Reading.station_id==chosen_id, Reading.parameter_id==145)
	count = get_count(events)
	return count,events


#Fetch all P POTEKA Readings in a day
#Input dateString of this format: 'YYYY-MM-DD' 
#for example if you want data of January 12, 2020, input '2020-01-12'
def fetchDayReadingsP(dateString):
	timeStart= datetime.strptime(dateString+' 00:00:00', '%Y-%m-%d %H:%M:%S')
	timeEnd = datetime.strptime(dateString+' 23:59:59' , '%Y-%m-%d %H:%M:%S')
	events = session.query(Reading).filter(Reading.time >= timeStart, Reading.time <= timeEnd, Reading.station_id.notin_(vStationLibrary))
	return events 

#Fetch all P POTEKA Readings in a day
#Input dateString of this format: 'YYYY-MM-DD' 
#for example if you want data of January 12, 2020, input '2020-01-12'
def fetchReadingsByTimeframeAndStationID(startDatetimeString,endDatetimeString,stationID):
	timeStart= datetime.strptime(startDatetimeString, '%Y-%m-%d %H:%M:%S')
	timeEnd = datetime.strptime(endDatetimeString , '%Y-%m-%d %H:%M:%S')
	events = session.query(Reading).filter(Reading.time >= timeStart, Reading.time <= timeEnd, Reading.station_id==stationID)
	return events 


#Fetch all Readings in a day for a single station given ID number
#Input dateString of this format: 'YYYY-MM-DD' 
#Input chosen_id corresponding to station id 
def fetchReadingByID(chosen_id):
	events = session.query(Reading).filter(Reading.reading_id==chosen_id)
	return events






