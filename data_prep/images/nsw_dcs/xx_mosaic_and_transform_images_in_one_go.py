
import boto3
import glob
import logging
import multiprocessing
import os
import pathlib
import platform

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from osgeo import gdal

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 20MB

s3_bucket = "bushfire-rasters"

if platform.system() == "Darwin":
    debug = False

    ram_to_use = 8

    input_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs/nsw_dcs_5m_dem")
    glob_pattern = "*/*-DEM-AHD_56_5m.asc"

    output_dem_file = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs", "nsw_dcs_5m_dem.tif")
    output_slope_file = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs", "nsw_dcs_5m_slope.tif")

else:
    debug = False

    ram_to_use = 480

    input_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs/nsw_dcs_5m_dem")
    glob_pattern = "*/*-DEM-AHD_56_5m.asc"

    output_dem_file = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs", "nsw_dcs_5m_dem.tif")
    output_slope_file = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs", "nsw_dcs_5m_slope.tif")


# how many parallel processes to run
max_processes = multiprocessing.cpu_count()

# set max RAM usage (divide by 4 as there are 4 processes - one per dataset)
gdal.SetCacheMax(int(ram_to_use / 4) * 1024 * 1024)


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START mosaic and transform images : {full_start_time}")

    create_dem()
    logger.info(f"\t - created DEM COG : {datetime.now() - start_time}")
    start_time = datetime.now()

    create_slope()
    logger.info(f"\t - created slope COG : {datetime.now() - start_time}")
    # start_time = datetime.now()

    logger.info(f"FINISHED mosaic and transform images : {datetime.now() - full_start_time}")


def create_dem():
    files_to_mosaic = list()

    # get images to mosaic and transform
    file_path = os.path.join(input_path, glob_pattern)
    files = glob.glob(file_path)
    num_images = len(files)

    if num_images > 0:
        files_to_mosaic.extend(files)
    else:
        print(f" - {file_path} has no images")

    # mosaic all merged files and output as a single Cloud Optimised GeoTIFF (COG) in GDA94 lat/long for all of AU
    if len(files_to_mosaic) > 0:
        gdal.Warp(output_dem_file, files_to_mosaic, format="COG",
                  options="-overwrite -multi -wm 80% -t_srs EPSG:4283 -co TILED=YES -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")
    else:
        print(f" - no files to merge")


def create_slope():
    gdal.DEMProcessing(output_slope_file, output_dem_file, "slope", scale=111120, format="COG",
                       options="-overwrite -multi -wm 80% -t_srs EPSG:4283 -co TILED=YES -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")


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
