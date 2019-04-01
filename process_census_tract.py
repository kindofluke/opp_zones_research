from ftplib import FTP
from pathlib import Path
import tempfile
import geopandas as gpd
import zipfile

crs = {'init' :'epsg:4326'}

def ftp_login():
    global ftp
    ftp = FTP('ftp2.census.gov')
    ftp.login()
    print("Logged into Census Succesfully")
    ftp.cwd('geo/tiger/TIGER2017/TRACT/')


def get_filenames():
    filenames = ftp.nlst()
    return filenames

def download_file(filename):
    with tempfile.TemporaryDirectory() as temp_directory:
        zip_temp_file = tempfile.TemporaryFile()
        print(f"Downloading {filename}")
        ftp.retrbinary('RETR ' + filename, zip_temp_file.write)
        print(f'Success {filename}')
        zip_file = zipfile.ZipFile(zip_temp_file)
        zip_file.extractall(path=temp_directory)
        shape_files = Path(temp_directory).glob('*.shp')
        for shape in shape_files:
            print(f"Creating GeoJSON for {filename}")
            geo_json_contents = gpd.read_file(shape).to_crs(crs).to_json()
            with open(Path(Path.cwd(),'data','census_tract_shapes',shape.stem + '.json'), 'w') as geojson_file:
                geojson_file.write(geo_json_contents)
        zip_temp_file.close()


ftp_login()
filenames = get_filenames()
for ftp_file in filenames:
    download_file(ftp_file)
ftp.quit()