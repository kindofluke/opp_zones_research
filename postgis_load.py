from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, Boolean, BigInteger
from geoalchemy2 import Geometry, Geography
from sqlalchemy.orm import sessionmaker
from pathlib import Path
engine = create_engine('postgresql://postgres@localhost/opp', echo=False)
import process_raw_census_data as prcd
import geopandas as gpd
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.shape import from_shape, WKBElement, WKTElement
from psycopg2.extensions import adapt, register_adapter, AsIs
from shapely.geometry import Polygon, MultiPolygon
import pandas as pd 


Base = declarative_base()

class GeoTract(Base):
    __tablename__ = 'geo_tract'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geoid = Column(String)
    ALAND = Column(Float)
    AWATER = Column(Float)
    matched_geoid = Column(String)
    geom = Column(Geography('POLYGON' , srid=4326))

class TractData(Base):
    __tablename__ = 'tract_data'
    geoid = Column(String, primary_key=True)
    median_income = Column(BigInteger)
    geoid2 = Column(String)
    pct_unemployment = Column(Float)
    pct_poverty = Column(Float)
    state_geoid = Column(String)
    poverty_above_20pct = Column(Boolean)
    ratio_state_median_income = Column(Float)
    ratio_msa_median_income = Column(Float, nullable=True)
    state_median_income_less_than_80pct = Column(Boolean)
    msa_median_income_less_than_80pct = Column(Boolean, nullable=True)
    qual_as_opp_zone = Column(Boolean)
    msa_median_income = Column(Float, nullable=True)
    median_income_state = Column(Float, nullable=True)

class IdentifedTracts(Base):
    __tablename__ = 'identified_tracts'
    id = Column(Integer, primary_key=True)
    geoid2 = Column(String)
    state = Column(String)


class QualifiedOppZones(Base):
    __tablename__ = 'qual_opp_zones'
    id = Column(Integer, primary_key=True)
    state = Column(String)
    county_name = Column(String)
    tract_geoid = Column(String)
    designation = Column(String)
    acs_range = Column(String)

def init_db():
    Base.metadata.create_all(engine)


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


def fix_multi(geometry):
    if type(geometry) == MultiPolygon:
        return None
    elif type(geometry) == Polygon:
        return geometry.wkt
    else:
        return None

def bulk_insert_mappings_geo():
    """Batched INSERT statements via the ORM "bulk", using dictionaries."""
    session = Session(bind=engine)
    session.bulk_insert_mappings(
        GeoTract,
        [
            dict(
                name=tract_file.NAME, ALAND=tract_file.ALAND, AWATER=tract_file.AWATER, geoid=tract_file.GEOID,
                matched_geoid=tract_file.matched_geoid, geom=fix_multi(tract_file.geometry)
            )
            for tract_file in getTract()
        ],
    )
    session.commit()


def bulk_insert_mappings_tractdata():
    all_tracts = prcd.isCensusTractQualified()
    all_tracts = all_tracts.dropna(subset=['pct_poverty','pct_unemployment','median_income'])#no null values 
    session = Session(bind=engine)
    session.bulk_insert_mappings(
        TractData,
        [
            dict(
                geoid=tract.geoid,
                median_income=prcd.fix_median_income(tract.median_income),
                geoid2 = tract.geoid2,
                pct_unemployment=tract.pct_unemployment,
                pct_poverty=tract.pct_poverty,
                state_geoid = tract.state_geoid,
                poverty_above_20pct = tract.poverty_above_20pct,
                median_income_state = tract.median_income_state,
                msa_median_income = tract.msa_median_income,
                ratio_state_median_income = tract.ratio_state_median_income,
                ratio_msa_median_income=tract.ratio_msa_median_income,
                state_median_income_less_than_80pct = tract.state_median_income_less_than_80pct,
                msa_median_income_less_than_80pct = tract.msa_median_income_less_than_80pct,
                qual_as_opp_zone = tract.qual_as_opp_zone




            )
            for tract in all_tracts.itertuples()
        ]
    )
    session.commit()

def bulk_insert_mappings_qualified_zone_data():
    path_to_file = Path('data','QualifiedOppZonesDec2018.csv')
    all_tracts = pd.read_csv(path_to_file, dtype={'TractID':str})
    all_tracts = all_tracts.dropna() #no null values 
    session = Session(bind=engine)
    session.bulk_insert_mappings(
        QualifiedOppZones,
        [
            dict(
         
                state = tract.State,
                county_name = tract.County,
                tract_geoid = tract.TractID,
                designation = tract.Designation,
                acs_range = tract.ACSYears

            )
            for tract in all_tracts.itertuples()
        ]
    )
    session.commit()

def bulk_insert_mappings_identified_tracts():
    path_to_file = Path('data','IdentifiedPossibleOppZones.csv')
    all_tracts = pd.read_csv(path_to_file)
    all_tracts = all_tracts.dropna() #no null values 
    session = Session(bind=engine)
    session.bulk_insert_mappings(
        IdentifedTracts,
        [
            dict(
         
                state = tract.State,
                geoid2 = tract.GEOID2

            )
            for tract in all_tracts.itertuples()
        ]
    )
    session.commit()
6
def load_all():
    init_db()
    bulk_insert_mappings_geo()
    bulk_insert_mappings_tractdata()
    bulk_insert_mappings_qualified_zone_data()
    bulk_insert_mappings_identified_tracts()


bulk_insert_mappings_tractdata()

