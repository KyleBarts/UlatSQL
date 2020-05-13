from base import Base

from sqlalchemy import Column, String, Integer, DateTime

class Reading(Base):
    __tablename__ = 'tbl_reading'
    __table_args__ = {'extend_existing': True}
    __table_args__ = {'schema' : 'ulat'}

    reading_id=Column(Integer, primary_key=True)
    time=Column('datetime_read', String)



    def __init__(self, datetime_read):
        self.datetime_read = datetime_read