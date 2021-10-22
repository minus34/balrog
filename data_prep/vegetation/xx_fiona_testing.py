
import fiona
import pyproj

from datetime import datetime
from shapely.geometry import Polygon, Point, mapping

from shapely.ops import transform

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
# print(str(buffer))
# print(mapping(buffer))

print(f"created buffer : {datetime.now() - start_time}")
start_time = datetime.now()

# open file and filter by buffer
with fiona.open("/Users/s57405/tmp/bushfire/nvis6_bal.fgb") as src:
# with fiona.open("s3://bushfire-rasters/vegetation/nvis6_bal.fgb") as src:
    # print(len(src))  # should be 3798424
    row_count = 0
    clipped_list = list()

    for f in src.filter(mask=dict_buffer):
        props_dict = dict()
        geom_dict = dict()

        # poly = iter(f)
        # print(f['geometry']['type'])
        # print(f['properties'])

        props = f['properties']
        geom = Polygon(f['geometry']['coordinates'][0]).intersection(buffer)

        print(f"{geom.type} : {props['bal_name']} : {props['area_m2']} m2")

        row_count += 1

print(f"Got {row_count} polygons : {datetime.now() - start_time}")


# Blue Mountains sample -- 150.387354,-33.730476, 150.401037,-33.720566



