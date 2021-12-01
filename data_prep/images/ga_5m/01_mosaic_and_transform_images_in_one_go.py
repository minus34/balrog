
import boto3
import concurrent.futures
import glob
import io
import logging
import multiprocessing
import os
import pathlib
import platform
import requests
import zipfile
# import validate_cloud_optimized_geotiff

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from osgeo import gdal
# from urllib.parse import urlparse

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 20MB

s3_bucket = "bushfire-rasters"


if platform.system() == "Darwin":
    debug = True

    # urls = ["https://portal.spatial.nsw.gov.au/download/dem/56/Sydney-DEM-AHD_56_5m.zip",
    #         "https://portal.spatial.nsw.gov.au/download/dem/56/Wollongong-DEM-AHD_56_5m.zip",
    #         "https://portal.spatial.nsw.gov.au/download/dem/56/Penrith-DEM-AHD_56_5m.zip",
    #         "https://portal.spatial.nsw.gov.au/download/dem/56/Katoomba-DEM-AHD_56_5m.zip",
    #         "https://portal.spatial.nsw.gov.au/download/dem/56/PortHacking-DEM-AHD_56_5m.zip",
    #         "https://portal.spatial.nsw.gov.au/download/dem/56/Burragorang-DEM-AHD_56_5m.zip"
    #         ]

    ram_to_use = 8

    # input_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/nsw_dcs/nsw_dcs_5m_dem")
    # glob_pattern = "*-DEM-AHD_56_5m.zip"

    output_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/ga_5m")
    temp_output_path = os.path.join(output_path, "tmp")

    output_dem_file = os.path.join(output_path, "test_ga_5m_dem.tif")
    output_slope_file = os.path.join(output_path, "test_ga_5m_slope.tif")

else:
    debug = False

    ram_to_use = 480

    output_path = os.path.join(pathlib.Path.home(), "/data")
    temp_output_path = os.path.join(output_path, "/data/tmp")

    output_dem_file = os.path.join(output_path, "ga_5m_dem.tif")
    output_slope_file = os.path.join(output_path, "ga_5m_slope.tif")

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
    urls = get_image_list()

    if len(urls) > 0:
        # download and unzip image files
        dem_files = get_image_files(urls)
        logger.info(f"\t - downloaded {len(dem_files)} DEM files : {datetime.now() - start_time}")
        start_time = datetime.now()

        # convert DEM images to slope
        slope_files = convert_to_slope(dem_files)
        logger.info(f"\t - created {len(slope_files)} temp slope files : {datetime.now() - start_time}")
        start_time = datetime.now()

        # # only use if process fails after this point and you need to restart
        # dem_files = get_image_list_from_disk("dem")
        # slope_files = get_image_list_from_disk("slope")

        # mosaic slope images and transform to GDA94 lat/long
        logger.info(f"\t - processing big slope COG")
        mosaic_and_transform(slope_files, "slope", output_slope_file)
        logger.info(f"\t - created slope COG : {datetime.now() - start_time}")
        start_time = datetime.now()

        # remove intermediate files
        for file in slope_files:
            os.remove(file)

        # mosaic DEM images and transform to GDA94 lat/long
        logger.info(f"\t - processing big DEM COG")
        mosaic_and_transform(dem_files, "dem", output_dem_file)
        logger.info(f"\t - created DEM COG : {datetime.now() - start_time}")

        # remove intermediate files
        for file in dem_files:
            os.remove(file)

    logger.info(f"FINISHED mosaic and transform images : {datetime.now() - full_start_time}")


def get_image_list_from_disk(image_type):
    """ backup function if you need to reference images already downloaded and processed """

    file_path = os.path.join(temp_output_path, image_type, "*.tif")
    return glob.glob(file_path)


def get_image_list():
    """ Get list of image files to mosaic and transform.
        Creates URLs with GDAL prefixes to enable downloading and reading of ZIP files directly"""

    files = list()

    # get file names and MGA zones from reference file
    with open("ga_dem_urls.txt", "r") as f:
        for file_url in f.read().splitlines():
            # file_name = os.path.basename(file_url).replace(".zip", ".tif")

            # # add image file name to URL so GDAL can read it
            # url = "/".join(["/vsizip//vsicurl", file_url, file_name])
            files.append(file_url)

    # if debugging, only process the first 2 files
    if debug:
        files = files[:2]

    num_images = len(files)

    if num_images > 0:
        logger.info(f"Processing {num_images} images")
    else:
        logger.warning(f"\t - nsw_elevation_index.csv has no images")

    return files


def get_image_files(urls):
    logger.info(f"\t - downloading images")

    image_files = list()

    i = 1

    start_time = datetime.now()

    with concurrent.futures.ProcessPoolExecutor(max_processes) as executor:
        futures = {executor.submit(download_and_unzip_image, url): url for url in urls}

    for fut in concurrent.futures.as_completed(futures):
        dem_file = fut.result()
        if dem_file is not None:
            image_files.append(dem_file)
            logger.info(f"\t\t - processed file {i}: {dem_file} : {datetime.now() - start_time}")
        else:
            logger.warning(f"\t\t - FAILED to process file {i}: {dem_file} : {datetime.now() - start_time}")

        i += 1

    return image_files


def download_and_unzip_image(url):
    file_name = os.path.basename(url).replace(".zip", ".tif")
    image_dir = os.path.join(temp_output_path, "dem")

    # download zip file
    response = requests.get(url)

    # Create a StringIO object, which behaves like a file
    string_buf = io.BytesIO(response.content)
    string_buf.seek(0)

    # Create a ZipFile object, instantiated with our file-like StringIO object.
    # Extract all of the data from that StringIO object into files in the provided output directory.
    myzip = zipfile.ZipFile(string_buf, 'r', zipfile.ZIP_DEFLATED)
    myzip.extractall(image_dir)
    myzip.close()
    string_buf.close()

    # return unzipped file path
    return os.path.join(image_dir, file_name)


def convert_to_slope(dem_files):
    logger.info(f"\t - creating slope images")

    slope_files = list()

    i = 1

    start_time = datetime.now()

    with concurrent.futures.ProcessPoolExecutor(max_processes) as executor:
        futures = {executor.submit(create_slope_image, dem_file): dem_file for dem_file in dem_files}

        for fut in concurrent.futures.as_completed(futures):
            slope_file = fut.result()
            if slope_file is not None:
                slope_files.append(slope_file)
                logger.info(f"\t\t - processed file {i}: {slope_file} : {datetime.now() - start_time}")
            else:
                logger.warning(f"\t\t - FAILED to process file {i}: {slope_file} : {datetime.now() - start_time}")

            i += 1

    return slope_files


def create_slope_image(dem_file):
    """ convert DEM to GeoTIFF and then to slope and output as a single GeoTIFF """

    try:
        # convert ASC format input DEM file to TIF -- not required as GA 5m DEMs are already uncompressed GeoTIFFs
        dem_file_name = os.path.basename(dem_file)
        # dem_file = os.path.join(temp_output_path, "dem", dem_file_name)
        #
        # gdal_dataset = gdal.Translate(dem_file, input_file,
        #                               options="-of GTiff -a_nodata -3.402823e+38 -co TILED=YES -co COMPRESS=DEFLATE "
        #                                       "-co BIGTIFF=YES -co NUM_THREADS=ALL_CPUS")
        # del gdal_dataset

        # convert DEM TIF to slope image
        slope_file_name = dem_file_name.replace(".tif", "-gdal_slope.tif")
        slope_file = os.path.join(temp_output_path, "slope", slope_file_name)

        gdal_dataset = gdal.DEMProcessing(slope_file, dem_file, "slope", alg="Horn",
                                          options="-of GTiff -co TILED=YES -co COMPRESS=DEFLATE "
                                                  "-co BIGTIFF=YES -co NUM_THREADS=ALL_CPUS")
        del gdal_dataset

        return slope_file
    except:
        return None


def mosaic_and_transform(files, image_type, output_file):
    start_time = datetime.now()

    temp_output_file = os.path.join(temp_output_path, f"temp_{image_type}.tif")

    # mosaic all merged files and output as a single GeoTIFF in GDA94 lat/long
    gdal_dataset = gdal.Warp(temp_output_file, files,
                             options="-of GTiff -overwrite -multi -wm 80% -t_srs EPSG:4283 "
                                     "-co BIGTIFF=YES -co TILED=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")
    del gdal_dataset

    logger.info(f"\t \t - created big {image_type} GeoTIFF : {datetime.now() - start_time}")

    # convert GeoTIFF file to a Cloud Optimised GeoTIFF file (COG)
    gdal_dataset = gdal.Translate(output_file, temp_output_file,
                                  options="-of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")
    del gdal_dataset

    # print(validate_cloud_optimized_geotiff.validate(output_file))

    # delete intermediate file
    os.remove(temp_output_file)

    # # build overviews
    # image = gdal.Open(output_file, 1)
    # gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
    # image.BuildOverviews('NEAREST', [4, 8, 16, 32, 64, 128, 256, 512], gdal.TermProgress_nocb)
    # del image


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
