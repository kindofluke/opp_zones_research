from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float
from geoalchemy2 import Geometry
from sqlalchemy.orm import sessionmaker
from pathlib import Path
engine = create_engine('postgresql://postgres@localhost/opp', echo=True)
import process_raw_census_data as prcd
import geopandas as gpd
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class GeoTract(Base):
    __tablename__ = 'geo_tract'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geoid = Column(String)
    ALAND = Column(Float)
    AWATER = Column(Float)
    matched_geoid = Column(String)
    geom = Column(Geometry('POLYGON'))

GeoTract.__table__.drop(engine)
GeoTract.__table__.create(engine)

Session = sessionmaker(bind=engine)
session = Session()

# all_tracts = prcd.isCensusTractQualified()
# all_tracts.set_index('geoid', inplace=True)

def getTract():
    json_file_paths = Path('data', 'census_tract_shapes').glob("*.json")
    for json_file in json_file_paths:
        state_file = gpd.read_file(json_file)
        state_file['matched_geoid'] = state_file.GEOID.apply(prcd.fix_geoid)
        for tract in state_file.itertuples():
            yield tract



def bulk_insert_mappings():
    """Batched INSERT statements via the ORM "bulk", using dictionaries."""
    session = Session(bind=engine)
    session.bulk_insert_mappings(
        GeoTract,
        [
            dict(
                name=tract_file.NAME, ALAND=tract_file.ALAND, AWATER=tract_file.AWATER, geoid=tract_file.GEOID,
                matched_geoid=tract_file.matched_geoid
            )
            for tract_file in getTract()
        ],
    )
    session.commit()

bulk_insert_mappings()