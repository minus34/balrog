
import boto3
import glob
import pathlib
import os

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from osgeo import gdal

full_start_time = datetime.now()
print(f"START - mosaic and transform images : {full_start_time}")

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 10MB

s3_bucket = "bushfire-rasters"

inputs = [{"name": "2m land cover",
           "input_path": "/data/geoscape/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 2M JUNE 2021/Standard",
           "output_file": "/data/geoscape/geoscape_2m_land_cover.tif",
           "s3_file_path": "geoscape/geoscape_2m_land_cover.tif",

           }

]


s3_file_path = "geoscape/geoscape_2m_land_cover.tif"

input_path = os.path.join(pathlib.Path.home(), "/data/geoscape/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 2M JUNE 2021/Standard")
output_file = os.path.join(pathlib.Path.home(), "tmp/bushfire/veg/geoscape_2m_land_cover.tif")

warped_files = list()

# mosaic and transform to WGS84 lat/long for each MGA zone (aka UTM South zones on GDA94 datum)
for zone in range(49, 57):
    start_time = datetime.now()

    print(f"Processing MGA Zone {zone}")

    files_to_mosaic = glob.glob(os.path.join(input_path, f"*_Z{zone}_*.tif"))
    vrt_file = os.path.join(input_path, f"temp_Z{zone}.vrt")
    interim_file = os.path.join(input_path, f"temp_Z{zone}.tif")

    my_vrt = gdal.BuildVRT(vrt_file, files_to_mosaic)
    my_vrt = None
    # print(f"\t - VRT created : {datetime.now() - start_time}")
    # start_time = datetime.now()

    gd = gdal.Warp(interim_file, vrt_file, format="GTiff", options="-t_srs EPSG:4326 -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS -overwrite")
    del gd
    os.remove(vrt_file)
    print(f"\t - mosaiced and transformed images : {datetime.now() - start_time}")

    warped_files.append(interim_file)

# mosaic all merged files and output as a single Cloud Optimised GeoTIFF (COG) for all of AU
start_time = datetime.now()
print(f"Processing AU")
vrt_file = os.path.join(input_path, "temp_au.vrt")
my_vrt = gdal.BuildVRT(os.path.join(input_path, "temp_au.vrt"), warped_files)
my_vrt = None
# print(f"\t - VRT created : {datetime.now() - start_time}")
# start_time = datetime.now()

gd = gdal.Warp(output_file, vrt_file, format="COG", options="-co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS -overwrite")
del gd
os.remove(vrt_file)
print(f"\t - mosaiced and transformed images : {datetime.now() - start_time}")
start_time = datetime.now()

# delete interim files
for file in warped_files:
    os.remove(file)

# upload to AWS S3
aws_response = s3_client.upload_file(output_file, s3_bucket, s3_file_path, Config=s3_config)
print(f"\t - image uploaded to s3 : {datetime.now() - start_time}")

print(f"FINISHED - mosaic and transform images : {datetime.now() - full_start_time}")
