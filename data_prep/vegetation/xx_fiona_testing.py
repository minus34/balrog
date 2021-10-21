
import fiona

from datetime import datetime
from shapely.geometry import Polygon


start_time = datetime.now()

with fiona.open("s3://bushfire-rasters/vegetation/nvis6_bal.fgb") as src:
    print(len(src))  # should be 1931443

    for f in src.filter(bbox=(150.387354,-33.730476, 150.401037,-33.720566)):
        poly = iter(f)
        # print(f['geometry']['type'])
        # print(f['properties'])

        props = f['properties']
        geom = Polygon(f['geometry']['coordinates'][0])

        print(f"{geom.type} : {props['bal_name']} : {props['area_m2']} m2")

print(f"it took {datetime.now() - start_time}")



# Blue Mountains sample -- 150.387354,-33.730476, 150.401037,-33.720566