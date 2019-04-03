shp2pgsql -d -S -G /code/data/Hospitals/Hospitals.shp | psql -U postgres -d opp
shp2pgsql -d -S -G /code/data/Urgent_Care_Facilities/Urgent_Care_Facilities.shp | psql -U postgres -d opp