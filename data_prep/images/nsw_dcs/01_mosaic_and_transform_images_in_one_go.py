
# downloads all NSW DCS DEM images and converts them to slope images. Then creates single mosaiced slope & DEM COGs
# input is either a locdl directory with DEM images in it or the list of tile names in the NSW map grid (in the nsw_elevation_index.csv file)
#
# Hugh Saalmans, IAG Firemark Labs, some time in mid-late 2021
#

import boto3
import concurrent.futures
import glob
import logging
import multiprocessing
import os
import pathlib
import platform
# import validate_cloud_optimized_geotiff

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from osgeo import gdal
# from urllib.parse import urlparse

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 20MB
s3_bucket = "bushfire-rasters"

base_url = "https://portal.spatial.nsw.gov.au/download/dem"


if platform.system() == "Darwin":
    debug = True

    ram_to_use = 8

    output_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs")
    temp_output_path = os.path.join(output_path, "tmp")

    output_dem_file = os.path.join(output_path, "test_nsw_dcs_5m_dem.tif")
    output_slope_file = os.path.join(output_path, "test_nsw_dcs_5m_slope.tif")

else:
    debug = False

    ram_to_use = 480

    output_path = os.path.join(pathlib.Path.home(), "/data")
    temp_output_path = os.path.join(output_path, "/data/tmp")

    output_dem_file = os.path.join(output_path, "nsw_dcs_5m_dem.tif")
    output_slope_file = os.path.join(output_path, "nsw_dcs_5m_slope.tif")

# how many parallel processes to run
max_processes = int(multiprocessing.cpu_count() / 2)

# set max RAM usage
gdal.SetCacheMax(int(ram_to_use) * 1024 * 1024)

# create output paths if they doesn't exist
pathlib.Path(os.path.join(temp_output_path, "dem")).mkdir(parents=True, exist_ok=True)
pathlib.Path(os.path.join(temp_output_path, "slope")).mkdir(parents=True, exist_ok=True)


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START mosaic and transform images : {full_start_time}")

    # list of DEM files to process
    files = get_image_list()

    if len(files) > 0:
        # convert DEM images to slope
        dem_files, slope_files = convert_to_slope(files)
        # dem_files, slope_files = get_image_list_from_disk()  # only used if process fails after this point
        logger.info(f"\t - created {len(slope_files)} temp slope files : {datetime.now() - start_time}")
        start_time = datetime.now()

        # mosaic slope images and transform to GDA94 lat/long
        logger.info(f"\t - processing big slope COG")
        mosaic_and_transform(slope_files, output_slope_file)
        logger.info(f"\t - created slope COG : {datetime.now() - start_time}")
        start_time = datetime.now()

        # mosaic DEM images and transform to GDA94 lat/long
        logger.info(f"\t - processing big DEM COG")
        mosaic_and_transform(dem_files, output_dem_file)
        logger.info(f"\t - created DEM COG : {datetime.now() - start_time}")

        # remove temp files
        for file in dem_files:
            os.remove(file)
        for file in slope_files:
            os.remove(file)

    logger.info(f"FINISHED mosaic and transform images : {datetime.now() - full_start_time}")


def get_image_list_from_disk():
    """ backup function if you need to reference images already downloaded and processed """

    dem_files = list()
    slope_files = list()

    file_path = os.path.join(temp_output_path, "*/*.tif")
    files = glob.glob(file_path)

    for file in files:
        if "-DEM-" in file:
            dem_files.append(file)
        elif "-gdal_slope-" in file:
            slope_files.append(file)

    return dem_files, slope_files


def get_image_list():
    """ Get list of image files to mosaic and transform.
        Creates URLs with GDAL prefixes to enable downloading and reading of ZIP files directly"""

    files = list()

    # get file names and MGA zones from reference file
    with open("nsw_elevation_index.csv", "r") as f:
        for line in f.read().splitlines():
            mga_zone, file_name = line.split(",")

            # Fix source data error
            if file_name == "BaanBaa-DEM-AHD_55_5m":
                mga_zone = "55"

            # add image file name to URL so GDAL can read it
            url = "/".join(["/vsizip//vsicurl", base_url, mga_zone, file_name + ".zip", file_name + ".asc"])
            files.append(url)

    # if debugging, only process the first 2 files
    if debug:
        files = files[:2]

    num_images = len(files)

    if num_images > 0:
        logger.info(f"\t - processing {num_images} images")
    else:
        logger.warning(f"\t - nsw_elevation_index.csv has no images")

    return files


def convert_to_slope(files):
    dem_files = list()
    slope_files = list()

    i = 1

    start_time = datetime.now()

    with concurrent.futures.ProcessPoolExecutor(max_processes) as executor:
        futures = {executor.submit(create_slope_image, input_file): input_file for input_file in files}

        for fut in concurrent.futures.as_completed(futures):
            dem_file, slope_file = fut.result()
            if dem_file is not None:
                dem_files.append(dem_file)
                slope_files.append(slope_file)
                logger.info(f"\t\t - processed file {i}: {dem_file} : {datetime.now() - start_time}")
            else:
                logger.warning(f"\t\t - FAILED to process file {i}: {dem_file} : {datetime.now() - start_time}")

            i += 1

    return dem_files, slope_files


def create_slope_image(input_file):
    """ convert DEM to GeoTIFF and then to slope and output as a single GeoTIFF """

    try:
        # convert ASC format input DEM file to TIF
        dem_file_name = os.path.basename(input_file).replace(".asc", ".tif")
        dem_file = os.path.join(temp_output_path, "dem", dem_file_name)

        gdal_dataset = gdal.Translate(dem_file, input_file, format="GTiff",
                                      options="-co COMPRESS=NONE -co NUM_THREADS=ALL_CPUS")
        del gdal_dataset

        # convert DEM TIF to slope image
        slope_file_name = dem_file_name.replace("-DEM-", "-gdal_slope-")
        slope_file = os.path.join(temp_output_path, "slope", slope_file_name)

        gdal_dataset = gdal.DEMProcessing(slope_file, dem_file, "slope", alg="Horn",
                                          options="-of GTiff -co COMPRESS=NONE -co NUM_THREADS=ALL_CPUS")
        del gdal_dataset

        return dem_file, slope_file
    except:
        return None, None


def mosaic_and_transform(files, output_file):
    # mosaic all merged files and output as a single GeoTIFF in GDA94 lat/long
    gdal_dataset = gdal.Warp(output_file, files,
                             options="-of COG -overwrite -multi -wm 80% -t_srs EPSG:4283 "
                                     "-co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")
    del gdal_dataset


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
