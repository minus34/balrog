
import boto3
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

from rasterio.session import AWSSession

# create AWS session object
aws_session = AWSSession(boto3.Session())

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# dem_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_dem_s.tif"
# aspect_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_aspect.tif"
# slope_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_slope.tif"
dem_file_path = "/data/tmp/cog/srtm_1sec_dem_s.tif"
aspect_file_path = "/data/tmp/cog/srtm_1sec_aspect.tif"
slope_file_path = "/data/tmp/cog/srtm_1sec_slope.tif"
# image_srid = 4326  # WGS84 lat/long
input_table = "bushfire.buildings"


# dem_file_path = "/data/tmp/cog/dem/Sydney-DEM-AHD_56_5m.tif"
# image_srid = 28356  # MGA (aka UTM South) Zone 56
# input_table = "bushfire.buildings_mga56"

output_table = "bushfire.bal_factors"

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

    # # get extents of raster (minus 100m to avoid masking issues at the edges)
    # minx = None
    # maxy = None
    # maxx = None
    # miny = None
    # 
    # with rasterio.open(dem_file_path) as raster:
    #     meta = raster.profile.data
    # 
    #     transform = meta["transform"]
    # 
    #     minx = transform.c + 100.0
    #     maxy = transform.f - 100.0
    #     maxx = minx + transform.a * meta["width"] - 100.0
    #     miny = maxy + transform.e * meta["height"] + 100.0
    #     # print([minx, miny, maxx, maxy])

    # get postgres connection from pool
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # clean out target table
    pg_cur.execute(f"truncate table {output_table}")

    # get input geometries & building ID
    sql = f"""select * from {input_table} limit 100"""
    # where st_intersects(geom, st_transform(ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 28356), 4283))
    pg_cur.execute(sql)

    # TODO: remove property bdys and use a line projected from the GNAF point in the direction of the aspect

    # get the rows as a list of dicts
    feature_list = list(pg_cur.fetchall())
    feature_count = len(feature_list)

    logger.info(f"\t - got {feature_count} buildings to process : {datetime.now() - start_time}")
    start_time = datetime.now()

    # create job list and process properties in parallel
    mp_job_list = list()

    if feature_list is not None:
        for feature in feature_list:
            mp_job_list.append(feature)

    mp_pool = multiprocessing.Pool(max_processes)
    mp_results = mp_pool.map_async(process_building, mp_job_list, chunksize=1)

    while not mp_results.ready():
        print(f"\rProperties remaining : {mp_results._number_left}", end="")
        sys.stdout.flush()
        time.sleep(10)

    print(f"\r\n", end="")
    real_results = mp_results.get()
    mp_pool.close()
    mp_pool.join()

    success_count = 0
    fail_count = 0

    for result in real_results:
        if result is None:
            logger.warning("A multiprocessing process failed!")
        elif result == "SUCCESS!":
            success_count += 1
        else:
            fail_count += 1
            logger.warning(result)

    logger.info(f"\t\t - {success_count} properties got data")
    if fail_count > 0:
        logger.info(f"\t\t - {fail_count} properties got NO data")

    # clean up postgres connection
    pg_cur.close()
    pg_pool.putconn(pg_conn)

    logger.info(f"FINISHED : Create BAL Factors - aspect, slope & elevation : {datetime.now() - full_start_time}")


def process_building(feature):

    bld_pid = feature["bld_pid"]

    # TODO: this is bodge but it's supposedly the Pythonic way
    output_dict = dict()
    output_dict["bld_pid"] = bld_pid

    try:
        output_dict = get_data(output_dict, feature, dem_file_path, "dem")
        output_dict = get_data(output_dict, feature, aspect_file_path, "aspect")
        output_dict = get_data(output_dict, feature, slope_file_path, "slope")

        # export result to Postgres
        insert_row(output_table, output_dict)

        return "SUCCESS!"
    except Exception as ex:
        return f"{bld_pid} FAILED! : {ex}"


# mask the image and get data from the non-masked area
def get_data(output_dict, feature, input_file, image_type):
    with rasterio.Env(aws_session):
        with rasterio.open(input_file, "r") as raster:
            for geom_field in ["geom", "buffer"]:
                print(f"{output_dict['bld_pid']} : {geom_field} : {image_type} : {feature[geom_field]}")

                # create mask
                masked_image, masked_transform = rasterio.mask.mask(raster, [feature[geom_field]], pad=True, crop=True)

                # print(f"{output_dict['bld_pid']} : {geom_field} : {image_type} : got mask")

                # get rid of nodata values and flatten array
                flat_array = masked_image[numpy.where(masked_image > -9999)].flatten()

                # only proceed if there's data
                if flat_array.size != 0:
                    # get stats across the masked image
                    min_value = numpy.min(flat_array)
                    max_value = numpy.max(flat_array)

                    # aspect is a special case - values could be on either side of 360 degrees
                    if image_type == "aspect":
                        if min_value < 90 and max_value > 270:
                            flat_array[(flat_array >= 0.0) & (flat_array < 90.0)] += 360.0

                        avg_value = numpy.mean(flat_array)
                        std_value = numpy.std(flat_array)
                        med_value = numpy.median(flat_array)

                        if avg_value > 360.0:
                            avg_value -= 360.0

                        if med_value > 360.0:
                            med_value -= 360.0

                    else:
                        avg_value = numpy.mean(flat_array)
                        std_value = numpy.std(flat_array)
                        med_value = numpy.median(flat_array)

                    # assign results to output
                    if geom_field == "geom":
                        geom_name = "bldg"
                    else:
                        geom_name = "100m"

                    output_dict[f"{image_type}_{geom_name}_min"] = int(min_value)
                    output_dict[f"{image_type}_{geom_name}_max"] = int(max_value)
                    output_dict[f"{image_type}_{geom_name}_avg"] = int(avg_value)
                    output_dict[f"{image_type}_{geom_name}_std"] = int(std_value)
                    output_dict[f"{image_type}_{geom_name}_med"] = int(med_value)

    return output_dict


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
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s %(message)s",
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
