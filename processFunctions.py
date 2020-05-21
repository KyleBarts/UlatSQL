from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import time
import pandas as pd

def per_hour_event_count(df):
    '''
    Takes in a dataframe with the columns station_id, and datetime_read
    Processes data to group them together by hour and provides count of events per hour

    '''
    #First Separate Datetime into Date, Hour, Minute Columns
    df[['Date','Time']] = df.datetime_read.str.split(expand=True) 
    df[['Hour', 'Minute']] = df.Time.str.split(":", n = 1, expand = True) 
    df = df.drop(columns=['parameter_id', 'reading','datetime_read','Time'])
    #Group Rows by Hour, add two new rows: count per hour and string of all events for that hour
    new = df.groupby(['station_id','Date','Hour'])['Minute'].agg(['count',' '.join]).reset_index()
    return new

def per_day_event_count(df):
    '''
    Takes in a dataframe with the columns station_id, and datetime_read
    Processes data to group them together by hour and provides count of events per hour
    
    '''
    #First Separate Datetime into Date, and Time columns
    df[['Date','Time']] = df.datetime_read.str.split(expand=True) 
    df = df.drop(columns=['parameter_id', 'reading','datetime_read'])
    #Group Rows by Date, add two new columns showing count per day and string
    new = df.groupby(['station_id','Date'])['Time'].agg(['count',' '.join]).reset_index()
    return new

def per_hour_event_count_no_events(df):
    '''
    Takes in a dataframe with the columns station_id, and datetime_read
    Processes data to group them together by hour and provides count of events per hour

    '''
    #First Separate Datetime into Date, Hour, Minute Columns
    df[['Date','Time']] = df.datetime_read.str.split(expand=True) 
    df[['Hour', 'Minute']] = df.Time.str.split(":", n = 1, expand = True) 
    df = df.drop(columns=['parameter_id', 'reading','datetime_read','Time'])
    #Group Rows by Hour, add two new rows: count per hour and string of all events for that hour
    new = df.groupby(['station_id','Date','Hour'])['Minute'].agg(['count',' '.join]).reset_index()
    new = new.drop(columns=['join'])
    return new

def per_day_event_count_no_events(df):
    '''
    Takes in a dataframe with the columns station_id, and datetime_read
    Processes data to group them together by hour and provides count of events per hour
    
    '''
    #First Separate Datetime into Date, and Time columns
    df[['Date','Time']] = df.datetime_read.str.split(expand=True) 
    df = df.drop(columns=['parameter_id', 'reading','datetime_read'])
    #Group Rows by Date, add two new columns showing count per day and string
    new = df.groupby(['station_id','Date'])['Time'].agg(['count',' '.join]).reset_index()
    new = new.drop(columns=['join'])
    return new