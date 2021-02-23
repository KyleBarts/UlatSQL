from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import numpy as np
import time
import pandas as pd

def drop_minutes(time):
    output_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(minute=0,second=0)
    return str(output_time)

def drop_seconds(time):
    output_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(second=0)
    return str(output_time)

def convert_PST(time):
    output_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S') + timedelta(hours=8)
    return str(output_time)

def convert_temp(in_temp):
    if(in_temp=="////" or in_temp=="ERR"):
        return '0'
    if('.' in in_temp):
        return in_temp
    return str(float(in_temp)/10)

def convert_pressure(in_pressure):
    if(in_pressure=="/////" or in_pressure=="ERR"):
        return '0'
    if('.' in in_pressure):
        return in_pressure
    return str(float(in_pressure)/10)

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

def rollup_every_15_mins(current_time):
    current_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
   # print(current_time)
    #current_time = current_time + timedelta(hours=8) #convert to PST
    current_day = current_time.day
    if current_time.second > 0:
        current_time = current_time.replace(second=0)
        current_time = current_time + timedelta(minutes=1)
    #print(minute)
    minute = current_time.minute
    if minute == 0: return current_time
    if minute <= 15: return current_time.replace(minute=15)
    if minute <= 30: return current_time.replace(minute=30)
    if minute <= 45: return current_time.replace(minute=45)
    if minute > 45: 
        current_time = current_time + timedelta(hours= 1)
        return current_time.replace(minute=0)
        
def append_microseconds_to_datetime(df):

    df['datetime'] = df['datetime_read'].apply(lambda x: np.datetime64(datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), 'ns' ))
    df['reading'] = df['reading'].apply(lambda x: float(x))
    #df['datetime_read'] = df.apply(lambda x:  (x.datetime_read + np.timedelta64(np.int32(x.reading*100),'ns')) ,axis=1)
    #df['datetime_read'] = df.apply(lambda x:  (x.datetime_read + np.timedelta64(np.int32((x.reading*10)*1000),'ns')) ,axis=1)
    df['datetime'] = df.apply(lambda x:  (x.datetime + np.timedelta64(np.int32((x.reading*10)*1000),'ns')) ,axis=1)
    df=df.drop(columns=['parameter_id','reading_id'])
    df=df.sort_values(by=['datetime'])
    #df['datetime_read'] = np.datetime64(df['datetime_read'])

    

    
    #df['datetime_read'] = df.apply(lambda x: datetime.strptime(x., '%Y-%m-%d %H:%M:%S') + timedelta(microseconds=100) , axis=1)
    #df['col_3'] = df.apply(lambda x: f(x.col_1, x.col_2), axis=1)

    return df

def append_time_diff(df):
    #Calculate time diff with event before
    df['time_difference_before'] = df['datetime'] - df['datetime'].shift()
    df['time_difference_before'] = df['time_difference_before'].apply(lambda x: x.total_seconds())
    
    #Calculate time diff with event after
    df['time_difference_after'] = df['datetime'].shift(-1) - df['datetime']
    df['time_difference_after'] = df['time_difference_after'].apply(lambda x: x.total_seconds())
    factor = 0.05
    df = df[(df['time_difference_before'] < factor) | (df['time_difference_after'] < factor)]
    #df['marker'] = df['time_difference_before']
    df['marker'] = df['time_difference_before'].apply(lambda x: 'Start' if x > factor else 'Mid')
    df['time elapsed'] = 0.0
    counter = 0.0
    df=df.reset_index()
    df['next_marker']=df['marker'].shift(-2)
    df['group_number']=0
    group_counter=0
    for index, row in df.iterrows():
        if (row['marker']=='Start'):
            counter = 0.0
            if(row['next_marker']=='Start'): 
                df.at[index,'marker'] = 'false'
                df.at[index+1,'marker'] = 'false'
            else:
                group_counter=group_counter+1
        else:
            counter = counter + row['time_difference_before']
        #print('time elapsed:' + str(counter))
        df.at[index,'group_number']=group_counter
        #print(df.at[index,'marker'])
        df.at[index,'time elapsed'] = counter
    df = df[(df['marker'] != 'false') ]
    
    df = df.reset_index()
    df=df.drop(columns=['next_marker', 'index'])

    #new = df.groupby(['group_number'])['datetime','station_id'].agg(['count',' '.join]).reset_index()
    new = df.groupby('group_number')[['datetime','station_id']].agg(lambda x: tuple(x)).applymap(list)
    new['# of vlf_events'] = new['datetime'].apply(lambda x: len(x))
    #new =new[(new['# of vlf_events'] > 3) ]
    new = new.reset_index()
    new = new.drop(columns=['group_number'])
    new['event_duration (seconds)'] = new['datetime'].apply(lambda x: (x[len(x)-1]-x[0]).total_seconds())
    #new = df.groupby(['group_number'])['datetime_read', 'station_id'].apply(lambda x: x.values.tolist())
    print(new)

    return new

def count_lightning_events_15mins(df):
    df['new_time'] = df['datetime_read'].apply(rollup_every_15_mins)
    print(df)
    df = df.groupby(['new_time','station_id',])['datetime_read'].agg('count').reset_index()
    print('hey')
    df = df.rename(columns={'datetime_read':'count','new_time':'datetime_read'})
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
    #df['datetime_read'] = df['datetime_read'].apply(convert_PST)
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

def join_station_name_to_row(row, station_details):
	current_serial = row['station_id']
	current_details = station_details[station_details['Serial']==current_serial]
	current_station_name = current_details['Station Name'].iloc[0]
	#current_station_name = current_details['Station Name']
	#print(current_station_name)
	row['Station Name'] = current_station_name
	#current_station_name = 'hello'
	return current_station_name

def return_lat_to_row(row, station_details):
	current_serial = row['station_id']
	current_details = station_details[station_details['station_id']==current_serial]
	current_lat = current_details['latitude'].iloc[0]
	#current_station_name = current_details['Station Name']
	#print(current_station_name)
	row['LAT'] = current_lat
	#current_station_name = 'hello'
	return current_lat

def return_lon_to_row(row, station_details):
	current_serial = row['station_id']
	current_details = station_details[station_details['station_id']==current_serial]
	current_lon = current_details['longitude'].iloc[0]
	#current_station_name = current_details['Station Name']
	#print(current_station_name)
	row['lon'] = current_lon
	#current_station_name = 'hello'
	return current_lon