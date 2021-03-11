from base import Base
import csv
import pprint
import pytz

from sqlalchemy import Column, String, Integer, DateTime, orm


old_timezone = pytz.timezone("UTC")
new_timezone = pytz.timezone("Asia/Manila")



#pprint.pprint(readingDetailsDict)

class Health(Base):
    __tablename__ = 'tbl_health'
    __table_args__ = {'extend_existing': True}
    __table_args__ = {'schema' : 'ulat'}

    health_id=Column(Integer, primary_key=True)
    time=Column('datetime_read', DateTime)
    station_id = Column('station_id', String)
    parameter_id = Column('parameter_id',Integer)
    health = Column('health',String)
    reading_name = 'none'



    def __init__(self, time):
        self.time = time
        self.station_id = station_id


    #Perform necessary setup to data right after fetch
    #In this case if the station fetched is found in our tuple of V Poteka Serial Numbers, we change its station type to V-Poteka
    @orm.reconstructor
    def init_on_load(self):
        stringID = str(self.health_id)
        
        new_timezone_timestamp = old_timezone.localize(self.time).astimezone(new_timezone) 
        #print(f'Hey im {stringID}. my old timestamp is {self.time} while my new adjusted time is {new_timezone_timestamp}')

            