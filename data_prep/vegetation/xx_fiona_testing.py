

import fiona

from datetime import datetime


start_time = datetime.now()

with fiona.open("s3://bushfire-rasters/vegetation/nvis6_bal.fgb") as src:
    print(len(src))  # should be 1931443
    f = next(iter(src))
    print(f['geometry']['type'])
    print(f['properties'])

print(f"it took {datetime.now() - start_time}")

