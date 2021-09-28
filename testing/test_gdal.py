
import glob
import logging
import math
import multiprocessing
import os
# import numpy as np
import pathlib
import platform
import psycopg2
import psycopg2.extras
import rasterio
import requests

from osgeo import gdal
from datetime import datetime
from psycopg2 import pool
from psycopg2.extensions import AsIs

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# # This needs to be refreshed every 12 hours
# gic_auth_token = ""


# The use_reference_data flag below determines 2 things
#
# if True:
#   GNAF PIDs will be assigned slope and aspect values
#
# if False:
#

use_reference_data = False

# input dem files path
input_path = os.path.join(script_dir, "input", "*.asc")
output_path = os.path.join(script_dir, "output")

output_table = "bushfire.risk_factors"

if use_reference_data:
    # reference tables
    gnaf_table = "data_science.address_principals_nsw"
    cad_table = "data_science.aus_cadastre_boundaries_nsw"
else:
    pass

# auto-select model & postgres settings to allow testing on both MocBook and EC2 GPU (G4) instances
if platform.system() == "Darwin":
    pg_connect_string = "dbname=geo host=localhost port=5432 user='postgres' password='password'"
else:
    pg_connect_string = "dbname=geo host=localhost port=5432 user='ec2-user' password='ec2-user'"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------


# how many parallel processes to run (only used for downloading images, hence can use 2x CPUs safely)
max_processes = multiprocessing.cpu_count()
max_postgres_connections = max_processes + 1

# create postgres connection pool (accessible across multiple processes)
pg_pool = psycopg2.pool.SimpleConnectionPool(1, max_postgres_connections, pg_connect_string)


def main():
    full_start_time = datetime.now()

    logger.info(f"START : get slope & aspect : {full_start_time}")

    # get postgres connection from pool
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # # clean out target tables
    # pg_cur.execute(f"truncate table {output_table}")

    sql = f"""select cad.jurisdiction_id, 
                     geojson polygon
              from {cad_table} as cad
              inner join {gnaf_table} as gnaf on st_intersects(gnaf.geom, cad.geom)
              where gnaf.gnaf_pid = 'fred'"""
    pg_cur.execute(sql)



    # # test download of GIC DTM tiles
    # latitude = -34.024461
    # longitude = 151.051168
    # zoom = 16
    # dtm_tile = download_gic_dtm(latitude, longitude, zoom)
    #
    # with open(os.path.join(output_path, "test_dtm_tile.tif"), "wb") as f:
    #     f.write(bytes(dtm_tile))

    # # Test processing of DEM to get slope and aspect
    # for file_path in glob.glob(input_path):
    #     # process_dem(file_path, "hillshade")
    #     # process_dem(file_path, "color-relief")
    #     process_dem(file_path, "slope")
    #     process_dem(file_path, "aspect")
    #
    #     print(f"Processed : {os.path.basename(file_path)}")

    #     slope = calculate_slope(dem_file_path)
    #     aspect = calculate_aspect(dem_file_path)

    # print(type(slope))
    # print(slope.dtype)
    # print(slope.shape)

    # clean up postgres connection
    pg_cur.close()
    pg_pool.putconn(pg_conn)

    logger.info(f"FINISHED : swimming pool labelling : {datetime.now() - full_start_time}")



def deg2num(lat_deg, lon_deg, zoom):
    """Converts lat/long coordinates and a zoom level to WMTS tile coordinates"""
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return xtile, ytile


def num2deg(xtile, ytile, zoom):
    """Converts WMTS tile coordinates and a zoom level to lat/long coordinates"""
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return lat_deg, lon_deg


def download_gic_dtm(latitude, longitude, zoom):
    tilex, tiley = deg2num(latitude, longitude, zoom)

    # https://tile.openstreetmap.org/17/120531/78723.png
    # https://tile.openstreetmap.org/16/60266/39361.png

    url = f"https://api.gic.org/images/GetDTMTile/{zoom}/{tilex}/{tiley}?token={gic_auth_token}"

    response = requests.get(url)

    return response.content


def process_dem(dem_file, output_type):
    # create path if it doesn't exist
    pathlib.Path(os.path.join(output_path, output_type)).mkdir(parents=True, exist_ok=True)

    output_file = os.path.join(output_path, output_type,
                               os.path.basename(dem_file).replace(".asc", f"_{output_type}.tif"))

    if output_type == "color-relief":
        gdal.DEMProcessing(output_file, dem_file, output_type,
                           colorFilename=os.path.join(script_dir, "colour_palette.txt"))
    else:
        gdal.DEMProcessing(output_file, dem_file, output_type)

    # with rasterio.open(f"_{output_type}") as dataset:
    #     slope=dataset.read(1)
    # return slope


if __name__ == "__main__":
    # setup logging
    logger = logging.getLogger()

    # set logger
    log_file = os.path.abspath(__file__).replace(".py", ".log")
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                        datefmt="%m/%d/%Y %I:%M:%S %p")

    # setup logger to write to screen as well as writing to log file
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)

    main()
