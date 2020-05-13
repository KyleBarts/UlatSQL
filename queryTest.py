# coding=utf-8

# 1 - imports
from reading import Reading
from base import Session
from station import Station

# 2 - extract a session
session = Session()

# 3 - extract all movies
stations = session.query(Station).all()

# 4 - print movies' details
print('\n### All stations:')
for station in stations:
    print(f'{station.station_id} is in {station.location}')
print('')