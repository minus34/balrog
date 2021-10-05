# ------------------------------------------------------------------------------------------------------------------
#
# Script downloads zipped ASCII Grid files, unzips them in-memory, converts them to a Cloud Optimised GeoTIFF (COG)
#    and uploads them to AWS S3
#
# ------------------------------------------------------------------------------------------------------------------

import boto3
import fiona
import logging
import multiprocessing
import os
import pathlib
import rasterio

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from rasterio.io import MemoryFile
from rio_cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from urllib.parse import urlparse

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

debug = False

base_image_url = "https://portal.spatial.nsw.gov.au/download"

elevation_index_zipfile = os.path.join(script_dir, "data/input/nsw_elevation_index.zip")
elevation_index_shapefile = "nsw_elevation_index.shp"

output_path = os.path.join(script_dir, "data/output")

s3_bucket = "bushfire-rasters"
s3_path = "nsw_dcs_spatial_services"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------

# how many parallel processes to run (only used for downloading images, hence can use 2x CPUs safely)
max_processes = multiprocessing.cpu_count()
max_postgres_connections = max_processes + 1

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 10MB


def main():
    full_start_time = datetime.now()

    logger.info(f"START : Export to COG : {full_start_time}")

    # create debug output directory
    if debug:
        pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    # DEBUGGING
    url = "https://portal.spatial.nsw.gov.au/download/dem/56/Sydney-DEM-AHD_56_5m.zip"



    # get list of files to download and convert
    file_list = list()
    for feature in fiona.open(f"zip://{elevation_index_zipfile}!{elevation_index_shapefile}"):
    # for feature in fiona.open("/nsw_elevation_index.shp", vfs=f"zip://{elevation_index_shapefile}"):
        properties = feature["properties"]

        file_list.append(properties["dems5mid"])
        file_list.append(properties["slope5mid"])
        file_list.append(properties["aspect5mid"])

    # convert file names into URLs
    url_list = list()

    for file_name in file_list:
        # get image type
        if "-DEM-" in file_name:
            image_type = "dem"
        elif "-SLP-" in file_name:
            image_type = "slope"
        elif "-ASP-" in file_name:
            image_type = "aspect"
        else:
            image_type = None
            print(f"What is this rubbish file name? : {file_name}")

        mga_zone = file_name.split("_")[1]

        if mga_zone not in ["54", "55", "56"]:
            mga_zone = None
            print(f"What is this rubbish MGA zone? : {file_name}")

        if image_type is not None or mga_zone is not None:
            url = f"{base_image_url}/{image_type}/{mga_zone}/{file_name}.zip"

            url_list.append(url)

    # download & convert file; then upload to S3
    convert_to_cog(url, debug)

    logger.info(f"FINISHED : Export to COG : {datetime.now() - full_start_time}")


def convert_to_cog(url, debug=False):
    """Takes a raster file URL, downloads it and outputs a cloud optimised tiff (COG) image to AWS S3"""

    start_time = datetime.now()

    # create a Virtual File System (VFS) URL if it's a .zip file
    #   this enables Rasterio to load the image from the downloaded .zip file directly
    #   example URL: 'zip+https://example.com/files.zip!/folder/file.tif'
    parsed_url = urlparse(url)
    if url.endswith(".zip"):
        input_file_name = os.path.basename(parsed_url.path).replace(".zip", ".asc")
        url = f"zip+{url}!{input_file_name}"
    else:
        input_file_name = os.path.basename(parsed_url.path)

    output_file_name = input_file_name.replace(".asc", ".tif")

    # create a COG profile to set compression on the output file
    dst_profile = cog_profiles.get("deflate")

    # Create the COG in-memory and save to S3
    with rasterio.open(url) as input_image:
        with MemoryFile() as output_image:
            cog_translate(input_image, output_image.name, dst_profile, in_memory=True, nodata=-9999)

            logger.info(f"\t - {input_file_name} downloaded & converted to COG: {datetime.now() - start_time}")
            start_time = datetime.now()

            # DEBUGGING
            if debug:
                with open(os.path.join(output_path, output_file_name), "wb") as f:
                    f.write(output_image.read())

                logger.info(f"\t - {output_file_name} saved locally: {datetime.now() - start_time}")
                start_time = datetime.now()

            # upload to AWS S3
            s3_file_path = f"{s3_path}/dem/{output_file_name}"
            aws_response = s3_client.upload_fileobj(output_image, s3_bucket, s3_file_path, Config=s3_config)

            if aws_response is not None:
                print("\t\t - WARNING: {} copy to S3 problem : {}".format(output_file_name, aws_response))

            logger.info(f"\t - {input_file_name} uploaded to S3: {datetime.now() - start_time}")


if __name__ == "__main__":
    # setup logging
    logger = logging.getLogger()

    # set logger
    log_file = os.path.abspath(__file__).replace(".py", ".log")
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
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
