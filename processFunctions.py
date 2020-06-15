from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import time
import pandas as pd

def drop_minutes(time):
    output_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(minute=0,second=0)
    return str(output_time)

def drop_seconds(time):
    output_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(second=0)
    return str(output_time)

def convert_temp(in_temp):
    if(in_temp=="////"):
        return '25.0'
    return str(float(in_temp)/10)

def convert_rain(in_rain):
    if(in_rain=="////"):
        return '0.0'
    return str(float(in_rain)/10)

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
    #df = df.drop(columns=['parameter_id', 'reading','datetime_read','Time'])
    df = df.drop(columns=['parameter_id', 'reading','Time'])
    #Group Rows by Hour, add two new rows: count per hour and string of all events for that hour
    new = df.groupby(['station_id','Date','Hour'])['Minute'].agg(['count',' '.join]).reset_index()
    new = new.drop(columns=['join'])
    return new

def per_min_temp(df):
    df['reading'] = df['reading'].apply(convert_temp)
    df = df.drop(columns=['parameter_id'])
    print(df)
    return df

def per_min_rain(df):
    df['reading'] = df['reading'].apply(convert_rain)
    df = df.drop(columns=['parameter_id'])
    print(df)
    return df


def new_per_hour_event_count_no_events(df):
    '''
    Takes in a dataframe with the columns station_id, and datetime_read
    Processes data to group them together by hour and provides count of events per hour

    '''

    df['datetime_read'] = df['datetime_read'].apply(drop_minutes)
    new = df.groupby(['station_id','datetime_read'])['reading'].agg(['count']).reset_index()
    print(new)
    
    return new

def per_min_event_count_no_events(df):
    '''
    Takes in a dataframe with the columns station_id, and datetime_read
    Processes data to group them together by hour and provides count of events per hour

    '''

    df['datetime_read'] = df['datetime_read'].apply(drop_seconds)
    new = df.groupby(['station_id','datetime_read'])['reading'].agg(['count']).reset_index()
    print(new)
    
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

def column_parser(row,time):
    if time in row['time']: 
        index_of_time = row['time'].index(time)
        reading = row['reading'][index_of_time]
        #print(f'can be found at {index_of_time} with reading {reading}')
        return reading
    else:
        return '0.0'

