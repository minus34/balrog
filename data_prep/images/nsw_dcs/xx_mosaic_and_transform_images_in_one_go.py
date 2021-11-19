
import boto3
import concurrent.futures
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
    glob_pattern = "*-DEM-AHD_56_5m.zip"

    output_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs")
    temp_output_path = os.path.join(output_path, "tmp")

    output_dem_file = os.path.join(output_path, "nsw_dcs_5m_dem.tif")
    output_slope_file = os.path.join(output_path, "nsw_dcs_5m_slope.tif")

else:
    debug = False

    ram_to_use = 480

    input_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs/nsw_dcs_5m_dem")
    glob_pattern = "*/*-DEM-AHD_56_5m.zip"

    output_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs")
    temp_output_path = os.path.join(output_path, "tmp")

    output_dem_file = os.path.join(output_path, "nsw_dcs_5m_dem.tif")
    output_slope_file = os.path.join(output_path, "nsw_dcs_5m_slope.tif")


# how many parallel processes to run
max_processes = multiprocessing.cpu_count()

# set max RAM usage (divide by 4 as there are 4 processes - one per dataset)
gdal.SetCacheMax(int(ram_to_use / 4) * 1024 * 1024)

# create output path if it doesn't exist
pathlib.Path(temp_output_path).mkdir(parents=True, exist_ok=True)


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START mosaic and transform images : {full_start_time}")

    # list of DEM files to process
    dem_files = get_image_list()

    if len(dem_files) > 0:
        # convert DEM images to slope
        slope_files = convert_to_slope(dem_files)
        logger.info(f"\t - created temp slope files : {datetime.now() - start_time}")
        start_time = datetime.now()

        # mosaic slope images and transform to GDA94 lat/long
        mosaic_and_transform(slope_files, output_slope_file)
        logger.info(f"\t - created slope COG : {datetime.now() - start_time}")
        start_time = datetime.now()

        # mosaic DEM images and transform to GDA94 lat/long
        mosaic_and_transform(dem_files, output_dem_file)
        logger.info(f"\t - created DEM COG : {datetime.now() - start_time}")

        # remove temp files
        for file in slope_files:
            os.remove(file)

    logger.info(f"FINISHED mosaic and transform images : {datetime.now() - full_start_time}")


def get_image_list():
    """ get list of image files to mosaic and transform"""
    file_path = os.path.join(input_path, glob_pattern)
    files = glob.glob(file_path)
    num_images = len(files)

    if num_images > 0:
        logger.info(f"\t - processing {num_images} images")
    else:
        logger.warning(f"\t - {file_path} has no images")

    return files


def convert_to_slope(dem_files):
    slope_files = list()

    with concurrent.futures.ProcessPoolExecutor(int(max_processes / 2)) as executor:
        futures = {executor.submit(create_slope_image, input_file): input_file for input_file in dem_files}

        for fut in concurrent.futures.as_completed(futures):
            output_file = fut.result()
            slope_files.append(output_file)

            logger.info(f"\t\t - created {output_file}")

    return slope_files


def create_slope_image(input_file):
    """ convert DEM to slope and output as a single Cloud Optimised GeoTIFF (COG) in GDA94 lat/long """
    file_name = os.path.basename(input_file).replace(".zip", ".tif").replace("-DEM-", "-gdal_slope-")
    output_file = os.path.join(temp_output_path, file_name)

    gdal.DEMProcessing(output_file, input_file, "slope", alg="Horn",
                       options="-of GTiff -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")

    return output_file


def mosaic_and_transform(files, output_file):
    # mosaic all merged files and output as a single Cloud Optimised GeoTIFF (COG) in GDA94 lat/long
    gdal.Warp(output_file, files, format="COG",
              options="-overwrite -multi -wm 80% -t_srs EPSG:4283 "
                      "-co TILED=YES -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")


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
