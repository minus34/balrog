
import fiona
import platform
import pyproj

from datetime import datetime
from shapely.geometry import Polygon, Point, mapping

from shapely.ops import transform

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







start_time = datetime.now()

# test coordinates
latitude = -33.730476
longitude = 150.387354

# get coordinate systems and transforms
wgs84 = pyproj.CRS('EPSG:4326')
lcc = pyproj.CRS('EPSG:3577')

project_2_lcc = pyproj.Transformer.from_crs(wgs84, lcc, always_xy=True).transform
project_2_wgs84 = pyproj.Transformer.from_crs(lcc, wgs84, always_xy=True).transform






# create input buffer polygon as both a WGS84 shape and a dict
wgs84_point = Point(longitude, latitude)
lcc_point = transform(project_2_lcc, wgs84_point)
buffer = transform(project_2_wgs84, lcc_point.buffer(250, cap_style=1))
dict_buffer = mapping(buffer)  # a dict representing a GeoJSON geometry

print(f"created buffer : {datetime.now() - start_time}")
start_time = datetime.now()

def process_veg():
    clipped_dict = props_dict
    clipped_dict["geom"] = geom

    print(f"{clipped_dict['geom'].type} : {clipped_dict['gid']} : {clipped_dict['bal_number']} : "
          f"{clipped_dict['bal_name']} : {clipped_dict['area_m2']} m2")


# open file and filter by buffer
with fiona.open(veg_file_path) as src:
    clipped_list = list()

    for f in src.filter(mask=dict_buffer):
        props_dict = f['properties']
        clipped_geom = Polygon(f['geometry']['coordinates'][0]).intersection(buffer)

        # need to cover cases where the clipped polygon creates a multipolygon
        if clipped_geom.type == "MultiPolygon":
            for geom in list(clipped_geom):
                clipped_list.append(process_veg())
        else:
            clipped_list.append(process_veg())

print(f"Got {len(clipped_list)} polygons : {datetime.now() - start_time}")


# Blue Mountains sample -- 150.387354,-33.730476, 150.401037,-33.720566



