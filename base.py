from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://kyle_bartido:r4AcMWKzd8EADc8G@202.90.158.249:5433/philsensor')
Session = sessionmaker(bind=engine)

Base = declarative_base()