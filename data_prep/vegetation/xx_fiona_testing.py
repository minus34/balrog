
import fiona
import io
import logging
import numpy
import os
import pathlib
import platform
import psycopg2.extras
import pyproj
import rasterio

from datetime import datetime
from osgeo import gdal
from rasterio import mask
from shapely import wkt
from shapely.geometry import Polygon, Point, LineString, mapping
from shapely.ops import nearest_points, transform
from typing import Optional, Any

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

# choose your settings for running locally or on a remote server
#   - edit this if not running locally on a Mac
if platform.system() == "Darwin":
    debug = True

    output_table = "bushfire.temp_veg"
    output_tablespace = "pg_default"
    postgres_user = "postgres"

    veg_file_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/veg/nvis6_bal.fgb")  # local
    # veg_file_path = "https://minus34.com/opendata/environment/nvis6_bal.fgb"  # over HTTP GetRange
    # veg_file_path = "s3://bushfire-rasters/vegetation/nvis6_bal.fgb"  # over S3 GetRange

    # Can't use HTTP GetRange - 20GB limit per file in AWS Cloudfront (file is 37GB)
    # dem_file_path = "https://minus34.com/opendata/ga/srtm_1sec_dem_s.tif"
    # dem_file_path = "s3://minus34.com/opendata/ga/srtm_1sec_dem_s.tif"  # over S3 GetRange
    dem_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_dem_s.tif"  # over S3 GetRange
    # aspect_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_aspect.tif"
    # slope_file_path = "s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_slope.tif"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='postgres' password='password'"
else:
    output_table = "bushfire.temp_veg"
    output_tablespace = "dataspace"
    postgres_user = "ec2-user"

    veg_file_path = "/data/nvis6_bal.fgb"
    dem_file_path = "/data/dem/geotiff/srtm_1sec_dem_s.tif"
    # aspect_file_path = "/data/dem/geotiff/srtm_1sec_aspect.tif"
    # slope_file_path = "/data/dem/geotiff/srtm_1sec_slope.tif"

    pg_connect_string = "dbname=geo host=localhost port=5432 user='ec2-user' password='ec2-user'"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------

# test coordinates

# # Sublime point, NSW
# latitude = -33.7345186
# longitude = 150.3393519

# Wentworth Falls, NSW
latitude = -33.7292483
longitude = 150.3861878

# # Marysville, VIC
# latitude = -37.5125066
# longitude = 145.7369306

buffer_size_m = 250.0

dem_resolution_m = 30.0

# get coordinate systems, geodetic parameters and transforms
geodesic = pyproj.Geod(ellps='WGS84')
wgs84_cs = pyproj.CRS('EPSG:4326')
lcc_proj = pyproj.CRS('EPSG:3577')
project_2_lcc = pyproj.Transformer.from_crs(wgs84_cs, lcc_proj, always_xy=True).transform
project_2_wgs84 = pyproj.Transformer.from_crs(lcc_proj, wgs84_cs, always_xy=True).transform


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START : Auto BAL Assessment : {full_start_time}")

    # get postgres connection
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # drop/create temp table
    sql = open("05_create_tables.sql", "r").read().format(postgres_user, output_table, output_tablespace)
    pg_cur.execute(sql)

    # create input buffer polygon as both a WGS84 shape and a dict
    wgs84_point = Point(longitude, latitude)
    lcc_point = transform(project_2_lcc, wgs84_point)
    buffer = transform(project_2_wgs84, lcc_point.buffer(buffer_size_m, cap_style=1))
    dict_buffer = mapping(buffer)  # a dict representing a GeoJSON geometry

    # create a larger buffer for aspect & slope calcs (need min of one pixel added to input buffer on all sides)
    dem_buffer = transform(project_2_wgs84, lcc_point.buffer(buffer_size_m + dem_resolution_m * 2.5, cap_style=1))

    logger.info(f"\t - created buffer : {datetime.now() - start_time}")
    start_time = datetime.now()

    # get elevation, aspect & slope data
    get_elevation_aspect_slope_files(dem_buffer)
    logger.info(f"\t - created elevation, aspect, slope files : {datetime.now() - start_time}")
    start_time = datetime.now()

    # get elevation, aspect & slope of the 100m buffer around inpit coordinates
    buffer_elevation = get_raster_values(buffer, "dem")
    buffer_aspect = get_raster_values(buffer, "aspect")
    buffer_slope = get_raster_values(buffer, "slope")
    logger.info(f"\t - got elevation, aspect, slope for buffer : {datetime.now() - start_time}")
    start_time = datetime.now()

    if debug:
        pg_cur.execute(f"insert into {output_table}_buffer "
                       f"values ({buffer_elevation} , {buffer_aspect}, {buffer_slope}, "
                       f"st_geomfromtext('{wkt.dumps(buffer)}', 4283))")

    # open vegetation file and filter by buffer
    with fiona.open(veg_file_path) as src:
        veg_list = list()

        for row in src.filter(mask=dict_buffer):
            # clip veg polygons by buffer
            veg_geom = Polygon(row['geometry']['coordinates'][0])
            clipped_geom = buffer.intersection(veg_geom)

            # need to cover cases where the clipped polygon creates a multipolygon
            if clipped_geom.type == "MultiPolygon":
                for geom in list(clipped_geom):
                    veg_dict = process_veg_polygon(geom, row, wgs84_point)

                    veg_list.append(veg_dict)
            else:
                veg_dict = process_veg_polygon(clipped_geom, row, wgs84_point)

                veg_list.append(veg_dict)

    # TODO: log Python OrderedDict bug

    logger.info(f"\t - got {len(veg_list)} polygons : {datetime.now() - start_time}")

    if debug:
        # export clipped veg polygons & nearest point lines to postgres
        export_list = list()
        for veg in veg_list:

            # print(veg["line"])

            veg["geom"] = wkt.dumps(veg["polygon"])
            veg["line_geom"] = wkt.dumps(veg["line"])
            veg.pop("polygon", None)
            veg.pop("line", None)

            export_list.append(veg)

        bulk_insert(veg_list)

    logger.info(f"FINISHED : Auto BAL Assessment : {datetime.now() - full_start_time}")



def get_elevation_aspect_slope_files(dem_buffer):
    with rasterio.Env():
        with rasterio.open(dem_file_path, "r") as src:
            # create mask using the large buffer
            dem_array, dem_transform = mask.mask(src, [dem_buffer], crop=True, nodata=-9999)

            # set profile of output dem file
            profile = src.profile
            profile.update(
                compress='deflate',
                driver='GTiff',
                height=dem_array.shape[1],
                width=dem_array.shape[2],
                nodata=-9999,
                transform=dem_transform
            )

            # save masked dem to file
            with rasterio.open('dem.tif', 'w', **profile) as dst:
                dst.write(dem_array)

            # convert masked dem to aspect & slope
            # note : scale is required to convert degrees to metres for calcs
            gdal.DEMProcessing("slope.tif", "dem.tif", "slope", scale=111120)
            gdal.DEMProcessing("aspect.tif", "dem.tif", "aspect", scale=111120)


def process_veg_polygon(geom, row, point):
    """Takes a vegetation polygon and determines its distance & bearing to the input coordinates,
         as well as it's median slope and aspect"""

    veg_dict = dict(row['properties'])  # convert from OrderDict: Python 3.9 bug appending to lists
    veg_dict["polygon"] = geom

    # get nearest point to input coordinates and the resulting line's geodesic distance and bearing
    points = nearest_points(point, geom)
    fwd_azimuth, back_azimuth, distance = geodesic.inv(points[0].x, points[0].y, points[1].x, points[1].y)

    # note: will return distance, bearing = 0 when coordinates are in vegetation
    veg_dict["azimuth"] = fwd_azimuth
    veg_dict["distance"] = distance
    veg_dict["line"] = LineString(points)

    # get slope & aspect
    veg_dict["aspect"] = get_raster_values(geom, "aspect")
    veg_dict["slope"] = get_raster_values(geom, "slope")

    # TODO: get elevation for each veg polygon AND a 100m buffer around input coordinates
    #   then determine if each veg polygon is above, level or below input cords
    #   also convert aspect to cardinal directions

    return veg_dict


def get_raster_values(geom, image_type):
    with rasterio.open(f"{image_type}.tif", "r") as src:
        masked_image, dem_transform = mask.mask(src, [geom], crop=True, nodata=-9999)
        # get rid of nodata values and flatten array
        flat_array = masked_image[numpy.where(masked_image > -9999)].flatten()

        if flat_array.size != 0:
            # get stats across the masked image
            min_value = numpy.min(flat_array)
            max_value = numpy.max(flat_array)

            # aspect is a special case - values could be either side of 360 degrees (North)
            if image_type == "aspect":
                if min_value < 90 and max_value > 270:
                    flat_array[(flat_array >= 0.0) & (flat_array < 90.0)] += 360.0

                med_value = int(numpy.median(flat_array))
                if med_value > 360:
                    med_value -= 360
            else:
                med_value = int(numpy.median(flat_array))

            return med_value
        else:
            return None


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
        logger.info(f"\t - Copy to Postgres FAILED! : {ex}")
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