from base import Base

from sqlalchemy import Column, String, Integer, Date

class Station(Base):
    __tablename__ = 'tbl_station'
    __table_args__ = {'extend_existing': True}
    __table_args__ = {'schema' : 'ulat'}

    station_id=Column(String, primary_key=True)
    location=Column('location', String)
    # barangay=Column('barangay', String(32))
    # municipality=Column('municipality', String(32))
    # congressional=Column('congressional', String(32))
    # province=Column('province', String(32))
    # region=Column('region', String(32))
    # latitude=Column('latitude', Numeric)
    # longitude=Column('longitude', Numeric)


    def __init__(self, title):
        self.location = location