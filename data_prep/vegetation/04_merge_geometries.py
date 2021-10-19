
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
import shapely
import sys
import time

from datetime import datetime
from typing import Optional, Any

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# choose your settings for running locally or on a remote server
#   - edit this if not running locally on a Mac
if platform.system() == "Darwin":
    input_sql = """select bal_number,
                          bal_name,
                          geom
                   from bushfire.nvis6_exploded
                   where bal_number > -9999
                   order by bal_number"""

    output_table = "bushfire.nvis6_bal"
    output_tablespace = "pg_default"
    postgres_user = "postgres"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='postgres' password='password'"
else:
    input_sql = """select bal_number,
                          bal_name,
                          geom
                   from bushfire.nvis6_exploded
                   where bal_number > -9999
                   order by bal_number"""

    output_table = "bushfire.nvis6_bal"
    output_tablespace = "dataspace"
    postgres_user = "ec2-user"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='ec2-user' password='ec2-user'"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------

# how many parallel processes to run (max 7 as there are 7 BAL vegetation classes)
max_processes = multiprocessing.cpu_count() - 1  # take one off as this is CPU intensive and on-one likes a locked up machine
if max_processes > 7:
    max_processes = 7


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START : Merge vegetation polygons : using {max_processes} processes : {full_start_time}")

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # create target table (and schema if it doesn't exist)
    # WARNING: drops output table if exists
    schema_name = output_table.split(".")[0]
    pg_cur.execute(f'create schema if not exists {schema_name}; alter schema {schema_name} owner to "{postgres_user}";')
    sql = open("05_create_tables.sql", "r").read().format(postgres_user, output_table, output_tablespace)
    pg_cur.execute(sql)

    # get input geometries & building IDs (copy_to used for speed)
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
    bulk_insert_row_count = math.ceil(float(feature_count) / float(max_processes))

    # split jobs into groups of 1,000 records (to ease to load on Postgres) for multiprocessing
    mp_job_list = list(split_list(feature_list, bulk_insert_row_count))

    logger.info(f"\t - got {feature_count} buildings to process : {datetime.now() - start_time}")
    start_time = datetime.now()

    mp_pool = multiprocessing.Pool(max_processes)
    mp_results = mp_pool.map_async(process_building, mp_job_list, chunksize=1)  # use map_async to show progress

    while not mp_results.ready():
        print(f"\rProperties remaining : {mp_results._number_left * bulk_insert_row_count}", end="")
        sys.stdout.flush()
        time.sleep(10)

    # print(f"\r\n", end="")
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

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # delete records from output table with invalid values (if any)
    sql = f"""delete from {output_table}
                  where aspect_med = -9999
                      or slope_med = -9999 
                      or dem_med = -9999
                      """
    pg_cur.execute(sql)
    adjustment_count = pg_cur.rowcount

    # update output table's stats
    pg_cur.execute(f"ANALYSE {output_table}")

    # adjust results due to invalid values being removed
    if adjustment_count is not None:
        success_count -= adjustment_count
        fail_count += adjustment_count

    logger.info(f"\t\t - {success_count} properties got data")
    if fail_count > 0:
        logger.info(f"\t\t - {fail_count} properties got NO data")

    logger.info(f"\t - got BAL factors : {datetime.now() - start_time}")
    start_time = datetime.now()

    # add primary key
    sql = f"ALTER TABLE {output_table} ADD CONSTRAINT {output_table.split('.')[1]}_pkey PRIMARY KEY (id)"
    pg_cur.execute(sql)
    logger.info(f"\t - added primary key to {output_table} : {datetime.now() - start_time}")

    # clean up postgres connection
    pg_cur.close()
    pg_conn.close()

    logger.info(f"FINISHED : Merge vegetation polygons : {datetime.now() - full_start_time}")


def split_list(input_list, max_count):
    """Yields successive n-sized chunks from list"""
    for i in range(0, len(input_list), max_count):
        yield input_list[i:i + max_count]


def process_building(features):
    """for a set of features and a set of input rasters - mask using each geometry and return min/max/median values"""

    record_count = len(features)

    success_count = 0
    fail_count = 0

    output_list = list()

    with rasterio.Env(aws_session):
        # open the images
        raster_dem = rasterio.open(dem_file_path, "r")
        raster_aspect = rasterio.open(aspect_file_path, "r")
        raster_slope = rasterio.open(slope_file_path, "r")

        # expected feature format is [id:string, geometry:string representing a valid geojson geometry]
        for feature in features:
            try:
                id = feature[0]
                geom = json.loads(feature[1])

                output_dict = dict()
                output_dict["id"] = id

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
                    masked_image, masked_transform = rasterio.mask.mask(raster, [geom], crop=True)

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

                    else:
                        output_dict[f"{image_type}_min"] = -9999
                        output_dict[f"{image_type}_max"] = -9999
                        output_dict[f"{image_type}_avg"] = -9999
                        output_dict[f"{image_type}_std"] = -9999
                        output_dict[f"{image_type}_med"] = -9999

                output_list.append(output_dict)
                success_count += 1
            except Exception as ex:
                error_message = str(ex)
                if error_message != "Input shapes do not overlap raster.":
                    print(f"{id} FAILED! : {error_message}")
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
    """creates a file object of the results to insert many rows into Postgres efficiently"""

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    csv_file_like_object = io.StringIO()

    try:
        for result in results:
            csv_file_like_object.write('|'.join(map(clean_csv_value, (result.values()))) + '\n')

        csv_file_like_object.seek(0)

        # Psycopg2 bug workaround - add schema to postgres search path and only use table name in copy_from
        pg_cur.execute(f"SET search_path TO {output_table.split('.')[0]}, public")
        pg_cur.copy_from(csv_file_like_object, output_table.split('.')[1], sep='|')
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
