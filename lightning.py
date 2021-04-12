from base import Base
import csv
import pprint
import pytz

from sqlalchemy import Column, String, Integer, DateTime, orm, Numeric

#reader = csv.DictReader(open('reading_details.csv'))

old_timezone = pytz.timezone("UTC")
new_timezone = pytz.timezone("Asia/Manila")

#readingDetailsDict = []

# for line in reader:
#     readingDetailsDict.append(line)

#pprint.pprint(readingDetailsDict)

class Lightning(Base):
    __tablename__ = 'tbl_vlf'
    __table_args__ = {'extend_existing': True}
    __table_args__ = {'schema' : 'ulat'}

    id=Column(Integer, primary_key=True)
    time=Column('datetime', DateTime)
    station_id = Column('station_id', String)

    tps = Column('tps', Numeric)
    tpp = Column('tpp', Numeric)
    tpz = Column('tpz', Numeric)
    tpn = Column('tpn', Numeric)

    app = Column('app', Numeric)
    apn = Column('apn', Numeric)
    apf = Column('apf', Numeric)
    apc = Column('apc', Numeric)

    apc = Column('vtr', Numeric)



    def __init__(self, time):
        self.time = time
        self.station_id = station_id


    #Perform necessary setup to data right after fetch
    #In this case if the station fetched is found in our tuple of V Poteka Serial Numbers, we change its station type to V-Poteka
    @orm.reconstructor
    def init_on_load(self):
        stringID = str(self.reading_id)
        
        new_timezone_timestamp = old_timezone.localize(self.time).astimezone(new_timezone) 
        #print(f'Hey im {stringID}. my old timestamp is {self.time} while my new adjusted time is {new_timezone_timestamp}')

            