
import io
import logging
import multiprocessing
import os
import platform
import psycopg2
import psycopg2.extras
import sys
import time

from datetime import datetime
from pyproj import Geod
from shapely import wkt
from shapely.ops import unary_union
from typing import Optional, Any

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# choose your settings for running locally or on a remote server
#   - edit this if not running locally on a Mac
if platform.system() == "Darwin":
    output_tablespace = "pg_default"
    postgres_user = "postgres"
    pg_connect_string = f"dbname=geo host=localhost port=5432 user='{postgres_user}' password='password'"
else:
    output_tablespace = "dataspace"
    postgres_user = "ec2-user"
    pg_connect_string = f"dbname=geo host=localhost port=5432 user='{postgres_user}' password='ec2-user'"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------

input_table = "bushfire.nvis6_exploded"
output_table = "bushfire.nvis6_bal"

# create ellipsoid for area calcs (WGS84 is close enough for what we need)
geod = Geod(ellps="WGS84")

# how many parallel processes to run (max 7 as there are only 7 BAL vegetation classes)
max_processes = multiprocessing.cpu_count() - 1  # take one off as this is CPU intensive and no-one likes a locked up machine
if max_processes > 7:
    max_processes = 7


def main():
    full_start_time = datetime.now()

    logger.info(f"START : Merge vegetation polygons : using {max_processes} processes : {full_start_time}")

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    schema_name = output_table.split(".")[0]
    table_name = output_table.split(".")[1]

    # create target table (and schema if it doesn't exist)
    # WARNING: drops output table if exists
    pg_cur.execute(f'create schema if not exists {schema_name}; alter schema {schema_name} owner to "{postgres_user}";')
    sql = open("05_create_tables.sql", "r").read().format(postgres_user, output_table, output_tablespace)
    pg_cur.execute(sql)

    # clean up postgres connection
    pg_cur.close()
    pg_conn.close()

    # process each BAL vegetation class in it's own process
    mp_job_list = list(range(1, 8))
    # mp_job_list = [4]  # DEBUGGING

    mp_pool = multiprocessing.Pool(max_processes)
    mp_results = mp_pool.map_async(process_bal_class, mp_job_list, chunksize=1)  # use map_async to show progress

    while not mp_results.ready():
        print(f"\rBAL classes remaining : {mp_results._number_left}", end="")
        sys.stdout.flush()
        time.sleep(10)

    print(f"\r \n", end="")
    real_results = mp_results.get()
    mp_pool.close()
    mp_pool.join()

    success_count = 0
    fail_count = 0

    for result in real_results:
        if result is None:
            logger.warning("A multiprocessing process failed!")
        elif result:
            success_count += 1
        else:
            fail_count += 1

    start_time = datetime.now()

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # update output table's stats
    pg_cur.execute(f"ANALYSE {output_table}")

    # add indexes
    pg_cur.execute(f"ALTER TABLE {output_table} ADD CONSTRAINT {table_name}_pkey PRIMARY KEY (gid, bal_number)")
    pg_cur.execute(f"CREATE INDEX {table_name}_bal_number_idx ON {output_table} USING btree (bal_number)")
    pg_cur.execute(f"CREATE INDEX {table_name}_geom_idx ON {output_table} USING gist (geom)")
    pg_cur.execute(f"ALTER TABLE {output_table} CLUSTER ON {table_name}_geom_idx")

    logger.info(f"\t - {output_table} analysed & indexes added: {datetime.now() - start_time}")

    # clean up postgres connection
    pg_cur.close()
    pg_conn.close()

    logger.info(f"FINISHED : Merge vegetation polygons : {datetime.now() - full_start_time}")


def process_bal_class(bal_number):
    """for a set of polygons - combine them into one multipolygon, then split into polygons
       expected feature format is [id:string, geometry:string representing a valid geojson geometry]"""

    full_start_time = datetime.now()
    start_time = datetime.now()

    # get geometries from postgres
    features = get_features(bal_number)
    feature_count = len(features)

    output_list = list()
    geom_list = list()
    bal_number = features[0][0]
    bal_name = features[0][1]

    print(f" - {bal_name} : got {feature_count} vegetation polygons to process : {datetime.now() - start_time}")
    start_time = datetime.now()

    for feature in features:
        geom_list.append(wkt.loads(feature[2]))

    print(f" - {bal_name} : ready to merge polygons : {datetime.now() - start_time}")
    start_time = datetime.now()

    # merge all polygons into one multipolygon
    the_big_one = unary_union(geom_list)

    print(f" - {bal_name} : polygons merged : {datetime.now() - start_time}")
    start_time = datetime.now()

    # break the one multipolygon into polygons that don't touch & convert to Well Known Text (WKT),
    #   add areas & give them a sequential id
    gid = 0
    polygons = list(the_big_one)
    for polygon in list(the_big_one):
        area_m2 = abs(geod.geometry_area_perimeter(polygon)[0])
        output_list.append({"gid": gid, "bal_number": bal_number, "bal_name": bal_name,
                            "area_m2": area_m2, "geom": wkt.dumps(polygon)})
        gid += 1

    print(f" - {bal_name} : multipolygon split into {len(polygons)} polygons: {datetime.now() - start_time}")
    start_time = datetime.now()

    # export results to postgres
    result = bulk_insert(output_list)

    if result:
        print(f" - {bal_name} : polygons exported to PostGIS: {datetime.now() - start_time}")
        output = True
    else:
        output = False

    print(f" - {bal_name} : FINISHED in {datetime.now() - full_start_time}")


def get_features(bal_number):

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_cur = pg_conn.cursor()

    # get input geometries
    input_sql = f"""select bal_number,
                      bal_name,
                      st_astext(geom)
               from {input_table}
               where bal_number = {bal_number}"""

    input_file_object = io.StringIO()
    pg_cur.copy_expert(f"COPY ({input_sql}) TO STDOUT", input_file_object)
    input_file_object.seek(0)

    # convert file object to list of features
    feature_list = list()
    for line in input_file_object.readlines():
        feature_list.append(tuple(line.split("\t")))

    input_file_object.close()

    # clean up postgres connection
    pg_cur.close()
    pg_conn.close()

    return feature_list


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
