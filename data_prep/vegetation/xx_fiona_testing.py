
import fiona
import io
import logging
import os
import platform
import psycopg2.extras
import pyproj

from datetime import datetime
from shapely import wkt
from shapely.geometry import Polygon, Point, mapping
from shapely.ops import transform
from typing import Optional, Any

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# choose your settings for running locally or on a remote server
#   - edit this if not running locally on a Mac
if platform.system() == "Darwin":
    output_table = "bushfire.temp_veg"
    output_tablespace = "pg_default"
    postgres_user = "postgres"

    veg_file_path = "/Users/s57405/tmp/bushfire/nvis6_bal.fgb"
    # veg_file_path = "s3://bushfire-rasters/vegetation/nvis6_bal.fgb"
    dem_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_dem_s.tif"
    aspect_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_aspect.tif"
    slope_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_slope.tif"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='postgres' password='password'"
else:
    output_table = "bushfire.temp_veg"
    output_tablespace = "dataspace"
    postgres_user = "ec2-user"

    veg_file_path = "/data/nvis6_bal.fgb"
    dem_file_path = "/data/tmp/cog/srtm_1sec_dem_s.tif"
    aspect_file_path = "/data/tmp/cog/srtm_1sec_aspect.tif"
    slope_file_path = "/data/tmp/cog/srtm_1sec_slope.tif"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='ec2-user' password='ec2-user'"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------

# test coordinates
latitude = -33.730476
longitude = 150.387354

buffer_size_m = 250

# get coordinate systems and transforms
wgs84 = pyproj.CRS('EPSG:4326')
lcc = pyproj.CRS('EPSG:3577')

project_2_lcc = pyproj.Transformer.from_crs(wgs84, lcc, always_xy=True).transform
project_2_wgs84 = pyproj.Transformer.from_crs(lcc, wgs84, always_xy=True).transform


def main():
    start_time = datetime.now()

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # drop/create temp table
    sql = open("05_create_tables.sql", "r").read().format(postgres_user, output_table, output_tablespace)
    # sql = f"delete from {output_table} where bal_number = 4; analyse {output_table}"  # DEBUGGING
    pg_cur.execute(sql)

    # create input buffer polygon as both a WGS84 shape and a dict
    wgs84_point = Point(longitude, latitude)
    lcc_point = transform(project_2_lcc, wgs84_point)
    buffer = transform(project_2_wgs84, lcc_point.buffer(buffer_size_m, cap_style=1))
    dict_buffer = mapping(buffer)  # a dict representing a GeoJSON geometry

    print(f"created buffer : {datetime.now() - start_time}")
    start_time = datetime.now()

    # open vegetation file and filter by buffer
    with fiona.open(veg_file_path) as src:
        clipped_list = list()

        for f in src.filter(mask=dict_buffer):
            # clip veg polygons by buffer
            clipped_geom = Polygon(f['geometry']['coordinates'][0]).intersection(buffer)

            # need to cover cases where the clipped polygon creates a multipolygon
            if clipped_geom.type == "MultiPolygon":
                for geom in list(clipped_geom):
                    clipped_list.append(process_veg(f['properties'], geom))
            else:
                clipped_list.append(process_veg(f['properties'], clipped_geom))

    print(f"Got {len(clipped_list)} polygons : {datetime.now() - start_time}")

    success = bulk_insert(clipped_list)

    print(success)


def process_veg(props, geom):
    """the party happens here"""
    clipped_dict = props
    clipped_dict["geom"] = wkt.dumps(geom)

    print(f"{geom.type} : {clipped_dict['gid']} : {clipped_dict['bal_number']} : "
          f"{clipped_dict['bal_name']} : {clipped_dict['area_m2']} m2")





    return clipped_dict


def bulk_insert(results):
    """creates a file object of the results to insert many rows into Postgres efficiently"""

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    csv_file_like_object = io.StringIO()

    try:
        for result in results:
            csv_file_like_object.write('|'.join(map(clean_csv_value, result.values())) + '\n')

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