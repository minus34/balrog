
import glob
# import json
import logging
import math
import multiprocessing
import os
import numpy
import pathlib
import platform
import psycopg2
import psycopg2.extras
import rasterio.mask
# import requests
import sys
import time

from osgeo import gdal
from datetime import datetime
from psycopg2 import pool
from psycopg2.extensions import AsIs

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# test_dem_path = "/Users/s57405/Downloads/PortHacking-DEM-AHD_56_5m/PortHacking-DEM-AHD_56_5m.asc"
# test_aspect_path = "/Users/s57405/Downloads/PortHacking-ASP-AHD_56_5m/PortHacking-ASP-AHD_56_5m.asc"
# test_slope_path = "/Users/s57405/Downloads/PortHacking-SLP-AHD_56_5m/PortHacking-SLP-AHD_56_5m.asc"

# # This needs to be refreshed every 12 hours
# gic_auth_token = ""

test_image_prefix = "PortHacking202004-LID1-AHD_3206234_56_0002_0002_1m"
image_types = ["aspect", "slope", "dem"]

# The flag below determines if slope and aspect values will be applied to GNAF address IDs
use_address_data = True

# input dem files path
input_path = os.path.join(script_dir, "input")
output_path = os.path.join(script_dir, "output")

output_table = "bushfire.bal_factors"

if use_address_data:
    # reference tables
    gnaf_table = "gnaf_202108.address_principals"
    cad_table = "data_science.aus_cadastre_boundaries_nsw"

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
    start_time = datetime.now()

    logger.info(f"START : Create BAL Factors - aspect, slope & elevation : {full_start_time}")

    dem_file_path = os.path.join(input_path, test_image_prefix + ".asc")

    # get bounding box of raster to filter properties by
    raster = gdal.Open(dem_file_path)
    geoTransform = raster.GetGeoTransform()

    minx = geoTransform[0]
    maxy = geoTransform[3]
    maxx = minx + geoTransform[1] * raster.RasterXSize
    miny = maxy + geoTransform[5] * raster.RasterYSize
    # print([minx, miny, maxx, maxy])

    raster = None

    # get postgres connection from pool
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # clean out target table
    pg_cur.execute(f"truncate table {output_table}")

    # TODO: this creates duplicate gnaf_pids - need to fix/avoid this
    # select rows with a GeoJSON geometry in teh same coordinate system as the rasters (MGA Zone 56)
    sql = f"""with gnaf as (
                  select gnaf_pid,
                         concat(address, ', ', locality_name, ' ', state, ' ', postcode) as address,
                         latitude,
                         longitude,
                         geom as point_geom
                  from {gnaf_table}
                  where coalesce(primary_secondary, 'P') = 'P'
                      --and gnaf_pid in ('GANSW705023300', 'GANSW705012493', 'GANSW705023298')
                      and st_intersects(geom, st_transform(ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 28356), 4283))
              )
              select cad.cad_pid,
                     gnaf.*,
                     st_asgeojson(st_transform(cad.geom, 28356), 1)::jsonb as geometry,
                     st_asgeojson(st_buffer(st_transform(cad.geom, 28356), 100.0), 1)::jsonb as buffer,
                     cad.geom
              from {cad_table} as cad
              inner join gnaf on st_intersects(gnaf.point_geom, cad.geom)"""
    pg_cur.execute(sql)
    # print(sql)

    # TODO: remove property bdys and use a line projected from the GNAF point in the direction of the aspect

    # get the rows as a list of dicts
    feature_list = list(pg_cur.fetchall())
    feature_count = len(feature_list)

    logger.info(f"\t - got {feature_count} properties to process : {datetime.now() - start_time}")
    start_time = datetime.now()

    # get aspect and slope rasters
    slope_path = process_dem(dem_file_path, "slope")
    aspect_path = process_dem(dem_file_path, "aspect")

    # TODO: remove this rasterio datasets can't be pickled across multiple threads/processes!
    #   Need to refactor so that an image is processed in one go, not each address record
    raster_dict = dict()
    raster_dict["dem"] = rasterio.open(dem_file_path)
    raster_dict["slope"] = rasterio.open(slope_path)
    raster_dict["aspect"] = rasterio.open(aspect_path)

    logger.info(f"\t - created aspect & slope rasters : {datetime.now() - start_time}")
    # start_time = datetime.now()

    # create job list and process properties in parallel
    mp_job_list = list()

    if feature_list is not None:
        for feature in feature_list:
            mp_job_list.append([dem_file_path, feature, image_types, test_image_prefix, raster_dict])

    mp_pool = multiprocessing.Pool(max_processes)
    mp_results = mp_pool.map_async(process_property, mp_job_list, chunksize=1)

    while not mp_results.ready():
        print(f"\rProperties remaining : {mp_results._number_left}", end="")
        sys.stdout.flush()
        time.sleep(1)

    print(f"\r\n", end="")
    real_results = mp_results.get()
    mp_pool.close()
    mp_pool.join()

    # mp_results = mp_pool.imap_unordered(process_property, mp_job_list)
    # mp_pool.close()
    # mp_pool.join()

    for result in real_results:
        if result != "SUCCESS!":
            logger.warning("A multiprocessing process failed!")

    # with rasterio.open(f"_{output_type}") as dataset:
    #     slope=dataset.read(1)
    # return slope


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

    logger.info(f"FINISHED : Create BAL Factors - aspect, slope & elevation : {datetime.now() - full_start_time}")


def process_property(job):
    start_time = datetime.now()

    dem_file_path = job[0]
    feature = job[1]
    image_types = job[2]
    test_image_prefix = job[3]
    raster_dict = job[4]

    gnaf_pid = feature["gnaf_pid"]

    output_dict = dict()
    output_dict["cad_pid"] = feature["cad_pid"]
    output_dict["gnaf_pid"] = gnaf_pid
    output_dict["address"] = feature["address"]
    output_dict["latitude"] = feature["latitude"]
    output_dict["longitude"] = feature["longitude"]
    output_dict["point_geom"] = feature["point_geom"]
    output_dict["geom"] = feature["geom"]

    for image_type in image_types:
        if image_type == "dem":
            input_file = dem_file_path
        else:
            input_file = os.path.join(output_path, image_type, test_image_prefix + f"_{image_type}.tif")

        with raster_dict[image_type] as raster:
            raster_metadata = raster.meta.copy()

            for geom_field in ["geometry", "buffer"]:
                if image_type == "dem":
                    output_file = input_file.replace(".asc", f"_{gnaf_pid}_{geom_field}.tif")
                else:
                    output_file = input_file.replace(".tif", f"_{gnaf_pid}_{geom_field}.tif")

                # create mask
                masked_image, masked_transform = rasterio.mask.mask(raster, [feature[geom_field]], crop=True)

                # get rid of nodata values and flatten array
                flat_array = masked_image[numpy.where(masked_image > -9999)].flatten()

                # get stats across the masked image
                min_value = numpy.min(flat_array)
                max_value = numpy.max(flat_array)

                # aspect is a special case - values could be on either side of 360 degrees
                if image_type == "aspect":
                    if min_value < 90 and max_value > 270:
                        flat_array[(flat_array >= 0.0) & (flat_array < 90.0)] += 360.0

                    avg_value = numpy.mean(flat_array)
                    std_value = numpy.std(flat_array)

                    if avg_value > 360.0:
                        avg_value -= 360.0
                else:
                    avg_value = numpy.mean(flat_array)
                    std_value = numpy.std(flat_array)

                med_value = numpy.median(flat_array)

                # assign results to output
                if geom_field == "geometry":
                    geom_name = "bdy"
                else:
                    geom_name = "100m"

                output_dict[f"{image_type}_{geom_name}_min"] = int(min_value)
                output_dict[f"{image_type}_{geom_name}_max"] = int(max_value)
                output_dict[f"{image_type}_{geom_name}_avg"] = int(avg_value)
                output_dict[f"{image_type}_{geom_name}_std"] = int(std_value)
                output_dict[f"{image_type}_{geom_name}_med"] = int(med_value)

                # # output image (optional)
                # raster_metadata.update(driver="GTiff",
                #                        height=int(masked_image.shape[1]),
                #                        width=int(masked_image.shape[2]),
                #                        nodata=-9999, transform=masked_transform, compress='lzw')
                #
                # with rasterio.open(output_file, "w", **raster_metadata) as masked_raster:
                #     masked_raster.write(masked_image)

    # export result to Postgres
    insert_row(output_table, output_dict)
    # print(output_dict)

    # # faux logging due to being in a separate process
    # print(f"root        : INFO     \t\t - processed {gnaf_pid} : {datetime.now() - start_time}")

    return "SUCCESS!"


# def nan_if(arr, value):
#     """Replaces all values with NaN in a numpy array"""
#     return numpy.where(arr == value, numpy.nan, arr)
#
#
# def deg2num(lat_deg, lon_deg, zoom):
#     """Converts lat/long coordinates and a zoom level to WMTS tile coordinates"""
#     lat_rad = math.radians(lat_deg)
#     n = 2.0 ** zoom
#     xtile = int((lon_deg + 180.0) / 360.0 * n)
#     ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
#     return xtile, ytile
#
#
# def num2deg(xtile, ytile, zoom):
#     """Converts WMTS tile coordinates and a zoom level to lat/long coordinates"""
#     n = 2.0 ** zoom
#     lon_deg = xtile / n * 360.0 - 180.0
#     lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
#     lat_deg = math.degrees(lat_rad)
#     return lat_deg, lon_deg
#
#
# def download_gic_dtm(latitude, longitude, zoom):
#     tilex, tiley = deg2num(latitude, longitude, zoom)
#
#     # https://tile.openstreetmap.org/17/120531/78723.png
#     # https://tile.openstreetmap.org/16/60266/39361.png

# https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_5M_Elevation/ImageServer/WMTS/1.0.0/16/60266/39361.png


#     url = f"https://api.gic.org/images/GetDTMTile/{zoom}/{tilex}/{tiley}?token={gic_auth_token}"
#
#     response = requests.get(url)
#
#     return response.content


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

    # "COMPRESS=LZW"

    # with rasterio.open(f"_{output_type}") as dataset:
    #     slope=dataset.read(1)
    # return slope

    return output_file

def insert_row(table_name, row):
    """Inserts a python dictionary as a new row into a database table.
    Allows for any number of columns and types; but column names and types MUST match existing columns"""

    # get postgres connection from pool
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # get column names & values (dict keys must match existing table columns)
    columns = list(row.keys())
    values = [row[column] for column in columns]

    insert_statement = f"INSERT INTO {table_name} (%s) VALUES %s"
    sql = pg_cur.mogrify(insert_statement, (AsIs(','.join(columns)), tuple(values))).decode("utf-8")
    pg_cur.execute(sql)

    # clean up postgres connection
    pg_cur.close()
    pg_pool.putconn(pg_conn)


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