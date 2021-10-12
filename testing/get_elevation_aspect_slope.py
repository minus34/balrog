
import boto3
import csv
import glob
import io
import json
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
from typing import Iterator, Optional, Any, Dict

# create AWS session object
aws_session = AWSSession(boto3.Session())

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

image_types = ["dem", "aspect", "slope"]

# image_srid = 4326  # WGS84 lat/long

# dem_file_path = "/data/tmp/cog/dem/Sydney-DEM-AHD_56_5m.tif"
# image_srid = 28356  # MGA (aka UTM South) Zone 56
# input_table = "bushfire.buildings_mga56"

output_table = "bushfire.bal_factors"

# auto-select model & postgres settings to allow testing on both MocBook and EC2 GPU (G4) instances
if platform.system() == "Darwin":
    bulk_insert_row_count = 10000

    input_table = "bushfire.buildings_sydney"

    dem_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_dem_s.tif"
    aspect_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_aspect.tif"
    slope_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_slope.tif"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='postgres' password='password'"
else:
    bulk_insert_row_count = 100000

    input_table = "bushfire.buildings"

    dem_file_path = "/data/tmp/cog/srtm_1sec_dem_s.tif"
    aspect_file_path = "/data/tmp/cog/srtm_1sec_aspect.tif"
    slope_file_path = "/data/tmp/cog/srtm_1sec_slope.tif"

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

    logger.info(f"START : Create BAL Factors - aspect, slope & elevation : using {max_processes} processes : {full_start_time}")

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

    # feature_list = None

    input_file_object = io.StringIO()

    # get postgres connection from pool
    with get_cursor() as pg_cur:
        # clean out target table
        pg_cur.execute(f"truncate table {output_table}")

        # Psycopg2 bug workaround - set schema to search path and only use table name in copy_from
        pg_cur.execute(f"SET search_path TO {input_table.split('.')[0]}, public")

        # get input geometries & building IDs (copy_to used for speed)
        pg_cur.copy_to(input_file_object, input_table.split('.')[1], sep="|")

        # # get input geometries & building ID
        # sql = f"""select * from {input_table}"""
        # # where st_intersects(geom, st_transform(ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 28356), 4283))
        # pg_cur.execute(sql)
        #
        # # get the rows as a list of dicts
        # feature_list = list(pg_cur.fetchall())

    logger.info(f"\t - got data from Postgres : {datetime.now() - start_time}")
    start_time = datetime.now()

    # # convert CSV rows to as list (using json.loads as it's fast)
    # feature_list = json.loads(input_file_object.read())

    # convert file object to list
    input_file_object.seek(0)
    feature_list = list()
    for line in input_file_object.readlines():
        feature_list.append(line.split("|"))
        # list(map(int, stringValue.split(' ')))

    # split jobs into groups of 1,000 records (to ease to load on Postgres) for multiprocessing
    mp_job_list = list(split_list(feature_list, bulk_insert_row_count))

    feature_count = len(feature_list)

    logger.info(f"\t - got {feature_count} buildings to process : {datetime.now() - start_time}")
    start_time = datetime.now()

    mp_pool = multiprocessing.Pool(max_processes)
    mp_results = mp_pool.map_async(process_building, mp_job_list, chunksize=1)

    while not mp_results.ready():
        # TODO: this will initially show too many records being processed - ignore
        print(f"\rProperties remaining : {mp_results._number_left * bulk_insert_row_count}", end="")
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
        else:
            success_count += result[0]
            fail_count += result[1]

    logger.info(f"\t\t - {success_count} properties got data")
    if fail_count > 0:
        logger.info(f"\t\t - {fail_count} properties got NO data")

    logger.info(f"FINISHED : Create BAL Factors - aspect, slope & elevation : {datetime.now() - full_start_time}")


def split_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def process_building(features):
    """for a set of features and a set of input rasters - mask using each geometry and return min/max/median values"""

    record_count = features

    success_count = 0
    fail_count = 0

    output_list = list()

    with rasterio.Env(aws_session):
        # open the images
        raster_dem = rasterio.open(dem_file_path, "r")
        raster_aspect = rasterio.open(aspect_file_path, "r")
        raster_slope = rasterio.open(slope_file_path, "r")

        # feature format is [id, geometry]
        for feature in features:
            try:
                bld_pid = feature[0]
                geom = json.loads(feature[1])

                output_dict = dict()
                output_dict["bld_pid"] = bld_pid

                for image_type in image_types:
                    # set input to use
                    if image_type == "dem":
                        raster = raster_dem
                    elif image_type == "aspect":
                        raster = raster_aspect
                    elif image_type == "slope":
                        raster = raster_slope
                    else:
                        print(f"FAILED! : Invalid image type")
                        exit()

                    # TODO: clean this up
                    # for geom_field in ["geom", "buffer"]:
                    for geom_field in ["buffer"]:
                        # print(f"{output_dict['bld_pid']} : {geom_field} : {image_type} : {feature[1]}")

                        # create mask
                        masked_image, masked_transform = rasterio.mask.mask(raster, [geom], crop=True)

                        # print(f"{output_dict['bld_pid']} : {geom_field} : {image_type} : got mask")

                        # get rid of nodata values and flatten array
                        flat_array = masked_image[numpy.where(masked_image > -9999)].flatten()
                        del masked_image, masked_transform

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

                        else:
                            output_dict[f"{image_type}_{geom_name}_min"] = -9999
                            output_dict[f"{image_type}_{geom_name}_max"] = -9999
                            output_dict[f"{image_type}_{geom_name}_avg"] = -9999
                            output_dict[f"{image_type}_{geom_name}_std"] = -9999
                            output_dict[f"{image_type}_{geom_name}_med"] = -9999

                output_list.append(output_dict)
                success_count += 1
            except Exception as ex:
                error_message = str(ex)
                if error_message != "Input shapes do not overlap raster.":
                    print(f"{bld_pid} FAILED! : {error_message}")
                fail_count += 1

    # copy results to Postgres table
    if len(output_list) > 0:
        copy_result = bulk_insert(output_list)

        if copy_result:
            return (success_count, fail_count)
        else:
            # if the copy failed flag all features as failed
            return (0, record_count)
    else:
        # total failure!?
        return (0, record_count)


# def bulk_insert(results: Iterator[Dict[str, Any]]) -> None:
def bulk_insert(results):
    """creates a CSV like file object of the results to insert many rows into Postgres very quickly"""

    # get postgres connection from pool
    # pg_conn = pg_pool.getconn()
    # pg_conn.autocommit = True

    with get_cursor() as pg_cur:
        csv_file_like_object = io.StringIO()

        try:
            for result in results:
                csv_file_like_object.write('|'.join(map(clean_csv_value, (result.values()))) + '\n')

            csv_file_like_object.seek(0)

            # Psycopg2 bug workaround - set schema to search path and only use table name in copy_from
            pg_cur.execute(f"SET search_path TO {output_table.split('.')[0]}, public")
            pg_cur.copy_from(csv_file_like_object, output_table.split('.')[1], sep='|')

        except Exception as ex:
            print(f"Copy to Postgres FAILED! : {ex}")

            # pg_cur.close()
            # pg_pool.putconn(pg_conn)

            return False

    # pg_pool.putconn(pg_conn)

    return True


# @contextmanager
def get_cursor():
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True

    try:
        yield pg_conn.cursor
    finally:
        pg_pool.putconn(pg_conn)

def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


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
