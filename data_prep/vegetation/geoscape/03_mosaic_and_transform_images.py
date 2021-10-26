
import boto3
import glob
import os
import platform

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from osgeo import gdal

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 20MB

s3_bucket = "bushfire-rasters"

if platform.system() == "Darwin":
    input_list = [{"name": "2m land cover",
               "input_path": "/Users/s57405/Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 2M JUNE 2021/Standard",
               "output_file": "/Users/s57405/tmp/bushfire/veg/geoscape_2m_land_cover.tif",
               "s3_file_path": "geoscape/geoscape_2m_land_cover.tif"},
              {"name": "30m land cover",
               "input_path": "/Users/s57405/Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 30M JUNE 2021/Standard",
               "output_file": "/Users/s57405/tmp/bushfire/veg/geoscape_30m_land_cover.tif",
               "s3_file_path": "geoscape/geoscape_30m_land_cover.tif"},
              {"name": "trees",
               "input_path": "/Users/s57405/Downloads/Trees_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Trees/Trees JUNE 2021/Standard",
               "output_file": "/Users/s57405/tmp/bushfire/veg/geoscape_trees.tif",
               "s3_file_path": "geoscape/geoscape_trees.tif"}]
else:
    input_list = [{"name": "2m land cover",
                   "input_path": "/data/geoscape/Surface Cover/Surface Cover 2M JUNE 2021/Standard",
                   "output_file": "/data/geoscape/geoscape_2m_land_cover.tif",
                   "s3_file_path": "geoscape/geoscape_2m_land_cover.tif"},
                  {"name": "30m land cover",
                   "input_path": "/data/geoscape/Surface Cover/Surface Cover 30M JUNE 2021/Standard",
                   "output_file": "/data/geoscape/geoscape_30m_land_cover.tif",
                   "s3_file_path": "geoscape/geoscape_30m_land_cover.tif"},
                  {"name": "trees",
                   "input_path": "/data/geoscape/Trees/Trees JUNE 2021/Standard",
                   "output_file": "/data/geoscape/geoscape_trees.tif",
                   "s3_file_path": "geoscape/geoscape_trees.tif"}]

# process 1 dataset at a time using parallel processing (built into GDAL)
for input_dict in input_list:
    full_start_time = datetime.now()
    warped_files = list()

    print(f"START - {input_dict['name']} - mosaic and transform images : {full_start_time}")
    print(f"Processing MGA Zones")

    # mosaic and transform to WGS84 lat/long for each MGA zone (aka UTM South zones on GDA94 datum)
    for zone in range(49, 57):
        start_time = datetime.now()

        files_to_mosaic = glob.glob(os.path.join(input_dict["input_path"], f"*_Z{zone}_*.tif"))
        # vrt_file = os.path.join(input_dict["input_path"], f"temp_Z{zone}.vrt")
        interim_file = os.path.join(input_dict["input_path"], f"temp_Z{zone}.tif")

        # my_vrt = gdal.BuildVRT(vrt_file, files_to_mosaic)
        # my_vrt = None

        gd = gdal.Warp(interim_file, files_to_mosaic, format="GTiff", options="-r cubic -multi -wm 80% -t_srs EPSG:4326 -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS -overwrite")
        del gd
        # os.remove(vrt_file)

        warped_files.append(interim_file)

        print(f"\t- zone {zone} done : {datetime.now() - start_time}")

    # mosaic all merged files and output as a single Cloud Optimised GeoTIFF (COG) for all of AU
    start_time = datetime.now()
    print(f"Processing AU")

    # vrt_file = os.path.join(input_dict["input_path"], "temp_au.vrt")
    # my_vrt = gdal.BuildVRT(os.path.join(input_dict["input_path"], "temp_au.vrt"), warped_files)
    # my_vrt = None

    gd = gdal.Warp(input_dict["output_file"], warped_files, format="COG", options="-multi -wm 80% -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS -overwrite")
    del gd
    # os.remove(vrt_file)

    print(f"\t- done : {datetime.now() - start_time}")
    start_time = datetime.now()

    # delete interim files
    for file in warped_files:
        os.remove(file)

    # upload to AWS S3
    aws_response = s3_client.upload_file(input_dict["output_file"], s3_bucket, input_dict["s3_file_path"], Config=s3_config)
    print(f"\t - {input_dict['name']} - image uploaded to s3 : {datetime.now() - start_time}")

    print(f"FINISHED - {input_dict['name']} - mosaic and transform images : {datetime.now() - full_start_time}")
