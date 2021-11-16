
import boto3
import concurrent.futures
import io
import json
import logging
import math
import multiprocessing
import os
import numpy
import platform
import psycopg2
import psycopg2.extras
# import pyproj
import rasterio.mask
# import shapely.geometry
# import shapely.ops
# import sys
# import time
# import uuid

from datetime import datetime
# from osgeo import gdal
from rasterio.session import AWSSession

from typing import Optional, Any

# create AWS session object to pull image data from S3
aws_session = AWSSession(boto3.Session())

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# the order of these cannot be changed (must match table column order)
# image_types = ["aspect", "slope", "dem"]  # Note: SRTM elevation has issues around narrow peninsulas and tall buildings
image_types = ["slope"]  # Note: SRTM elevation has issues around narrow peninsulas and tall buildings

# how many parallel processes to run
max_processes = multiprocessing.cpu_count()

# # get coordinate systems, geodetic parameters and transforms
# geodesic = pyproj.Geod(ellps="WGS84")
# wgs84_cs = pyproj.CRS("EPSG:4326")
# lcc_proj = pyproj.CRS("EPSG:3577")
# project_2_lcc = pyproj.Transformer.from_crs(wgs84_cs, lcc_proj, always_xy=True).transform
# project_2_wgs84 = pyproj.Transformer.from_crs(lcc_proj, wgs84_cs, always_xy=True).transform

buffer_size_m = 110.0
# dem_resolution_m = 30.0


# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# choose your settings for running locally or on a remote server
#   - edit this if not running locally on a Mac
if platform.system() == "Darwin":
    # input_sql = """select bld.bld_pid,
    #                       st_asgeojson(bld.geog, 6, 0)::text as buffer
    #                from bushfire.temp_building_buffers as bld
    #                inner join bushfire.buildings_sydney as syd on bld.bld_pid = syd.bld_pid"""
    # input_sql = """select bld.bld_pid,
    #                       st_asgeojson(st_transform(geom::geometry, 28356), 1, 0)::jsonb as buffer
    #                from bushfire.temp_building_buffers as bld
    #                inner join bushfire.buildings_sydney as syd on bld.bld_pid = syd.bld_pid"""
    # input_sql = f"""select gnaf_pid,
    #                       st_asgeojson(st_buffer(st_makepoint(lon, lat)::geography, {buffer_size_m}, 4), 6, 0)::text as buffer
    #                from bushfire.temp_point_buffers limit 100"""
    # st_asgeojson(st_buffer(st_makepoint(lon, lat)::geography, {buffer_size_m + dem_resolution_m * 3.0}, 4), 6, 0)::text as big_buffer
    input_sql = f"""select ext_geo_id, 
                          st_asgeojson(st_buffer(geom::geography, {buffer_size_m}, 4), 6, 0)::text as buffer
                   from bushfire.temp_mgrs_points"""

    output_table = "bushfire.bal_factors_mgrs_slope_only"
    output_tablespace = "pg_default"
    postgres_user = "postgres"

    # dem_file_path = "/Users/s57405/tmp/bushfire/srtm_1sec_dem_s.tif"
    slope_file_path = "/Users/s57405/tmp/bushfire/srtm_1sec_slope.tif"
    # dem_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/cog/srtm_1sec_dem_s.tif"
    # aspect_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/cog/srtm_1sec_aspect.tif"
    # slope_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/cog/srtm_1sec_slope.tif"
    # dem_file_path = "s3://bushfire-rasters/nsw_dcs_spatial_services/dem/Sydney-DEM-AHD_56_5m.tif"
    # aspect_file_path = "s3://bushfire-rasters/nsw_dcs_spatial_services/aspect/Sydney-ASP-AHD_56_5m.tif"
    # slope_file_path = "s3://bushfire-rasters/nsw_dcs_spatial_services/slope/Sydney-SLP-AHD_56_5m.tif"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='postgres' password='password'"
else:
    # input_sql = """select bld_pid,
    #                       st_asgeojson(st_buffer(geom::geography, 110, 4), 6, 0)::text as buffer
    #                from bushfire.temp_building_buffers"""
    input_sql = f"""select gnaf_pid, 
                          st_asgeojson(st_buffer(st_makepoint(lon, lat)::geography, {buffer_size_m}, 4), 6, 0)::text as buffer
                   from bushfire.temp_point_buffers"""
    # st_asgeojson(st_buffer(st_makepoint(lon, lat)::geography, {buffer_size_m + dem_resolution_m * 3.0}, 4), 6, 0)::text as big_buffer

    postgres_user = "ec2-user"
    output_table = "bushfire.bal_factors_gnaf_slope_only"
    output_tablespace = "dataspace"

    # dem_file_path = "/data/dem/cog/srtm_1sec_dem_s.tif"
    # aspect_file_path = "/data/dem/cog/srtm_1sec_aspect.tif"
    # slope_file_path = "/data/dem/cog/srtm_1sec_slope.tif"
    dem_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/cog/srtm_1sec_dem_s.tif"
    aspect_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/cog/srtm_1sec_aspect.tif"
    slope_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/cog/srtm_1sec_slope.tif"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='ec2-user' password='ec2-user'"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START : Create BAL Factors - aspect, slope & elevation : using {max_processes} processes : {full_start_time}")

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # create target table (and schema if it doesn't exist)
    # WARNING: drops output table if exists
    schema_name = output_table.split(".")[0]
    pg_cur.execute(f'create schema if not exists {schema_name}; alter schema {schema_name} owner to "{postgres_user}";')
    sql = open(os.path.join(script_dir, "03_create_tables.sql"), "r").read().format(postgres_user, output_table, output_tablespace)
    pg_cur.execute(sql)

    # get input geometries & IDs (copy_to used for speed)
    input_file_object = io.StringIO()
    pg_cur.copy_expert(f"COPY ({input_sql}) TO STDOUT", input_file_object)
    input_file_object.seek(0)

    # clean up postgres connection
    pg_cur.close()
    pg_conn.close()

    logger.info(f"\t - got data from Postgres : {datetime.now() - start_time}")
    start_time = datetime.now()

    # convert file object to list of features
    feature_list = list()
    for line in input_file_object.readlines():
        feature_list.append(tuple(line.split("\t")))

    input_file_object.close()

    # determine features per process (for multiprocessing)
    feature_count = len(feature_list)
    bulk_insert_row_count = math.ceil(float(feature_count) / float(max_processes * 4))
    if bulk_insert_row_count > 200000:
        bulk_insert_row_count = 200000

    # split jobs into groups of 1,000 records (to ease to load on Postgres) for multiprocessing
    mp_job_list = list(split_list(feature_list, bulk_insert_row_count))

    logger.info(f"\t - got {feature_count} records to process : {datetime.now() - start_time}")
    start_time = datetime.now()

    with concurrent.futures.ProcessPoolExecutor(int(max_processes / 2)) as executor:
        futures = {executor.submit(process_records, mp_job): mp_job for mp_job in mp_job_list}

        success_count = 0
        out_of_area_count = 0
        fail_count = 0

        for fut in concurrent.futures.as_completed(futures):
            result = fut.result()
            success_count += result[0]
            out_of_area_count += result[1]
            fail_count += result[2]

            print(f"\rRecords processed : {success_count} : "
                  f"outside of raster area : {out_of_area_count} : "
                  f"failures : {fail_count} : "
                  f"{datetime.now() - start_time}", end="")

        print("")

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # delete records from output table with invalid values (if any)
    # sql = f"""delete from {output_table}
    #               where slope_med = -9999
    #                   """
    # sql = f"""delete from {output_table}
    #               where aspect_med = -9999
    #                   or slope_med = -9999
    #                   or dem_med = -9999
    #                   """
    # pg_cur.execute(sql)
    # adjustment_count = pg_cur.rowcount

    # update output table's stats
    pg_cur.execute(f"ANALYSE {output_table}")

    # # adjust results due to invalid values being removed
    # if adjustment_count is not None:
    #     success_count -= adjustment_count
    #     fail_count += adjustment_count

    logger.info(f"\t\t - {success_count} records got data")
    if out_of_area_count > 0:
        logger.info(f"\t\t - {out_of_area_count} records were outside the raster area & got NO data")
    if fail_count > 0:
        logger.info(f"\t\t - {fail_count} records FAILED")

    logger.info(f"\t - got BAL factors : {datetime.now() - start_time}")
    start_time = datetime.now()

    # add primary key
    sql = f"ALTER TABLE {output_table} ADD CONSTRAINT {output_table.split('.')[1]}_pkey PRIMARY KEY (id)"
    pg_cur.execute(sql)
    logger.info(f"\t - added primary key to {output_table} : {datetime.now() - start_time}")

    # clean up postgres connection
    pg_cur.close()
    pg_conn.close()

    logger.info(f"FINISHED : Create BAL Factors - aspect, slope & elevation : {datetime.now() - full_start_time}")


def split_list(input_list, max_count):
    """Yields successive n-sized chunks from list"""
    for i in range(0, len(input_list), max_count):
        yield input_list[i:i + max_count]


def process_records(features):
    """for a set of features and a set of input rasters - mask using each geometry and return min/max/median values"""

    # process_id = uuid.uuid4()
    record_count = len(features)

    # print(f"{record_count} records")

    success_count = 0
    out_of_area_count = 0
    fail_count = 0

    output_list = list()

    with rasterio.Env(aws_session):
        # open the images
        # raster_dem = rasterio.open(dem_file_path, "r")
        # raster_aspect = rasterio.open(aspect_file_path, "r")
        raster_slope = rasterio.open(slope_file_path, "r")

        # expected feature format is [id:string, geometry:string representing a valid geojson geometry]
        for feature in features:
            try:
                id = feature[0]
                # latitude = float(feature[1])
                # longitude = float(feature[2])
                buffer = json.loads(feature[1])
                # dem_buffer = json.loads(feature[2])  # need oversized dem buffer to calc aspect & slope
                # print(f"{id} : start")

                # # create input buffer polygon as both a WGS84 shape and a dict
                # wgs84_point = shapely.geometry.Point(longitude, latitude)
                # # print(f"{id} : got wgs84 point")
                # # print(wgs84_point)
                #
                # lcc_point = shapely.ops.transform(project_2_lcc, wgs84_point)
                # # print(f"{id} : got lcc point")
                #
                # buffer = shapely.ops.transform(project_2_wgs84, lcc_point.buffer(buffer_size_m, cap_style=1))
                # # print(f"{id} : got buffer")
                #
                # # lcc_point = None
                # # wgs84_point = None
                #
                # # dict_buffer = shapely.ops.mapping(buffer)  # a dict representing a GeoJSON geometry
                #
                # # # create a larger buffer for aspect & slope calcs (need min of one pixel added to input buffer on all sides)
                # # dem_buffer = transform(project_2_wgs84, lcc_point.buffer(buffer_size_m + dem_resolution_m * 2.5, cap_style=1))

                output_dict = dict()
                output_dict["id"] = id

                # # create dem, slope and aspect images for this feature
                # get_elevation_aspect_slope_files(process_id, dem_buffer)

                for image_type in image_types:
                    # set input to use
                    if image_type == "dem":
                        raster = raster_dem
                    elif image_type == "aspect":
                        raster = raster_aspect
                    elif image_type == "slope":
                        raster = raster_slope
                    else:
                        print("FAILED! : Invalid image type")
                        exit()

                    # create mask
                    masked_image, masked_transform = rasterio.mask.mask(raster, [buffer], crop=True)
                    # print(f"{id} : {image_type} : got masked raster")

                    # get rid of nodata values and flatten array
                    flat_array = masked_image[numpy.where(masked_image > -9999)].flatten()
                    del masked_image, masked_transform

                    # only proceed if there's data
                    if flat_array.size != 0:
                        # get stats across the masked image
                        min_value = numpy.min(flat_array)
                        max_value = numpy.max(flat_array)

                        # aspect is a special case - values could be either side of 360 degrees (North)
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

                        # assign results to output (save space by converting these low precision values to integers)
                        output_dict[f"{image_type}_min"] = int(min_value)
                        output_dict[f"{image_type}_max"] = int(max_value)
                        output_dict[f"{image_type}_avg"] = int(avg_value)
                        output_dict[f"{image_type}_std"] = int(std_value)
                        output_dict[f"{image_type}_med"] = int(med_value)
                        output_dict[f"{image_type}_pixel_count"] = flat_array.size

                        # print(f"{id} : {image_type} : got raster stats")

                    else:
                        output_dict[f"{image_type}_min"] = -9999
                        output_dict[f"{image_type}_max"] = -9999
                        output_dict[f"{image_type}_avg"] = -9999
                        output_dict[f"{image_type}_std"] = -9999
                        output_dict[f"{image_type}_med"] = -9999
                        output_dict[f"{image_type}_pixel_count"] = -9999

                # del buffer

                output_list.append(output_dict)
                success_count += 1

                # print(f"{id} : done")

            except Exception as ex:
                error_message = str(ex)
                if error_message != "Input shapes do not overlap raster.":
                    print(f"{id} :  FAILED! : {error_message}")
                    fail_count += 1
                else:
                    out_of_area_count += 1

                    output_dict[f"{image_type}_min"] = -9999
                    output_dict[f"{image_type}_max"] = -9999
                    output_dict[f"{image_type}_avg"] = -9999
                    output_dict[f"{image_type}_std"] = -9999
                    output_dict[f"{image_type}_med"] = -9999
                    output_dict[f"{image_type}_pixel_count"] = -9999

    # copy results to Postgres table
    if len(output_list) > 0:
        copy_result = bulk_insert(output_list)

        if copy_result:
            return success_count, out_of_area_count, fail_count
        else:
            # if the copy failed flag all features as failed
            return 0, 0, record_count
    else:
        # total failure!?
        return 0, 0, record_count


# def get_elevation_aspect_slope_files(process_id, dem_buffer):
#     with rasterio.Env():
#         with rasterio.open(dem_file_path, "r") as src:
#             # create mask using the large buffer
#             dem_array, dem_transform = rasterio.mask.mask(src, [dem_buffer], crop=True, nodata=-9999)
#
#             # set profile of output dem file
#             profile = src.profile
#             profile.update(
#                 compress="deflate",
#                 driver="GTiff",
#                 height=dem_array.shape[1],
#                 width=dem_array.shape[2],
#                 nodata=-9999,
#                 transform=dem_transform
#             )
#
#             # save masked dem to file
#             with rasterio.open(f"{process_id}_dem.tif", "w", **profile) as dst:
#                 dst.write(dem_array)
#
#             # convert masked dem to aspect & slope
#             # note : scale is required to convert degrees to metres for calcs
#             gdal.DEMProcessing(f"{process_id}_slope.tif", f"{process_id}_dem.tif", "slope", scale=111120)
#             gdal.DEMProcessing(f"{process_id}_aspect.tif", f"{process_id}_dem.tif", "aspect", scale=111120)


# def bulk_insert(results: Iterator[Dict[str, Any]]) -> None:
def bulk_insert(results):
    """creates a file object of the results to insert many rows into Postgres efficiently"""

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    csv_file_like_object = io.StringIO()

    try:
        for result in results:
            csv_file_like_object.write("|".join(map(clean_csv_value, (result.values()))) + "\n")

        csv_file_like_object.seek(0)

        # Psycopg2 bug workaround - add schema to postgres search path and only use table name in copy_from
        pg_cur.execute(f"SET search_path TO {output_table.split('.')[0]}, public")
        pg_cur.copy_from(csv_file_like_object, output_table.split(".")[1], sep="|")
        output = True
    except Exception as ex:
        print(f"Copy to Postgres FAILED! : {ex}")
        output = False

    pg_cur.close()
    pg_conn.close()

    return output


def clean_csv_value(value: Optional[Any]) -> str:
    """Escape a couple of things that postgres won't like"""
    if value is None:
        return r"\N"
    return str(value).replace("\n", "\\n")


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
