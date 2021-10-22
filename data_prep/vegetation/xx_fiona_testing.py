
import fiona
import json
import pyproj

from datetime import datetime
from shapely import wkt
from shapely.geometry import Polygon, Point, mapping, shape

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

# create input buffer polygon
wgs84_point = Point(longitude, latitude)
lcc_point = transform(project_2_lcc, wgs84_point)

buffer = transform(project_2_wgs84, lcc_point.buffer(110, cap_style=1))
json_buffer = json.dumps(mapping(buffer))

# print(str(buffer))
# print(json.dumps(mapping(buffer)))

print(f"created buffer : {datetime.now() - start_time}")
start_time = datetime.now()

# open file and filter by buffer
with fiona.open("s3://bushfire-rasters/vegetation/nvis6_bal.fgb") as src:
    # print(len(src))  # should be 3798424

    row_count = 0

    for f in src.filter(mask=json_buffer):
    # for f in src.filter(bbox=(150.387354,-33.730476, 150.401037,-33.720566)):
        poly = iter(f)
        # print(f['geometry']['type'])
        # print(f['properties'])

        props = f['properties']
        geom = Polygon(f['geometry']['coordinates'][0])

        print(f"{geom.type} : {props['bal_name']} : {props['area_m2']} m2")

        row_count += 1

print(f"Got {row_count} polygons : {datetime.now() - start_time}")


# Blue Mountains sample -- 150.387354,-33.730476, 150.401037,-33.720566



