from ftplib import FTP
from pathlib import Path
import tempfile
import geopandas as gpd
import zipfile
from shapely.geometry import Polygon

crs = {'init' :'epsg:4326'}

def ftp_login():
    ftp = FTP('ftp2.census.gov')
    ftp.login()
    print("Logged into Census Succesfully")
    ftp.cwd('geo/tiger/TIGER2015/TRACT/')
    return ftp

def get_filenames():
    filenames = ftp.nlst()
    return filenames

def download_file(filename):
        zip_temp_file = tempfile.TemporaryFile()
        print(f"Downloading {filename}")
        ftp.retrbinary('RETR ' + filename, zip_temp_file.write)
        print(f'Success {filename}')
        zip_file = zipfile.ZipFile(zip_temp_file)
        zip_file.extractall(path=temp_directory)
        zip_temp_file.close()

def extract_shape_files():
        shape_files = Path(temp_directory).glob('*.shp')
        for shape in shape_files:
            print(f"Creating GeoJSON for {shape}")
            geojson_path = Path(Path.cwd(),'data','census_tract_shapes',shape.stem + '.json')
            frame = gpd.read_file(shape).to_crs(crs)
            frame[frame.geometry.apply(type) == Polygon].to_file(geojson_path, driver='GeoJSON')
                # geojson_file.write(geo_json_contents)


ftp = ftp_login()
filenames = get_filenames()
filter_files = [f for f in filenames]
with tempfile.TemporaryDirectory() as temp_directory:
    for ftp_file in filter_files:
        download_file(ftp_file)
    ftp.quit()
    extract_shape_files()
