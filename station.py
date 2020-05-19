from base import Base

from sqlalchemy import Column, String, Integer, Date, Numeric, orm

vStationLibrary = ('00173478','00174736','00181303','00181305','00181306','00181310')

class Station(Base):
    __tablename__ = 'tbl_station'
    __table_args__ = {'extend_existing': True}
    __table_args__ = {'schema' : 'ulat'}

    station_id=Column(String, primary_key=True)
    location=Column('location', String)
    latitude=Column('latitude', Numeric)
    longitude=Column('longitude', Numeric)
    #we set the default to P-POTEKA and change it using init_on_load if station is found in our V-POTEKA tuple
    station_type = 'P-POTEKA' 

    # Below are some variables found in the ULAT SQL DB but not necessary to our use cases yet
    # barangay=Column('barangay', String(32))
    # municipality=Column('municipality', String(32))
    # congressional=Column('congressional', String(32))
    # province=Column('province', String(32))
    # region=Column('region', String(32))



    def __init__(self):
        self.station_id = station_id
        self.location = location
        self.latitude = latitude
        self.longitude = longitude

    #Perform necessary setup to data right after fetch
    #In this case if the station fetched is found in our tuple of V Poteka Serial Numbers, we change its station type to V-Poteka
    @orm.reconstructor
    def init_on_load(self):
        if self.station_id in vStationLibrary:
            self.station_type = 'V-POTEKA'


