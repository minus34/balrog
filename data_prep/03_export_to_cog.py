# ------------------------------------------------------------------------------------------------------------------
#
# Script downloads zipped GeoTIFFs, unzips them in-memory, converts them to a Cloud Optimised GeoTIFF (COG)
#    and uploads them to AWS S3
#
# ------------------------------------------------------------------------------------------------------------------

import boto3
import json
import logging
import os
import pathlib
import rasterio
import sys
import time

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

urls_file = os.path.join(script_dir, "data/input/ga_dem_urls.json")

debug = False

output_path = os.path.join(script_dir, "data/output")

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 10MB


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START : Export to COG : {full_start_time}")

    # create debug output directory
    if debug:
        pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    # get list of files to convert
    url_list = json.loads(open(urls_file, "r").read())

    logger.info(f"\t - {len(url_list)} images to download & convert: {datetime.now() - start_time}")

    for file in url_list:
        convert_to_cog(file["url"], file["s3_bucket"], file["s3_path"])

    logger.info(f"FINISHED : Export to COG : {datetime.now() - full_start_time}")


def convert_to_cog(url, s3_bucket, s3_path):
    """Takes an image file URL, downloads it and outputs a cloud optimised tiff (COG) image to AWS S3"""

    start_time = datetime.now()

    # create a Virtual File System (VFS) URL if it's a .zip file
    #   this enables Rasterio to load the image from the downloaded .zip file directly
    #   example URL: 'zip+https://example.com/files.zip!/folder/file.tif'
    parsed_url = urlparse(url)
    if url.endswith(".zip"):
        input_file_name = os.path.basename(parsed_url.path).replace(".zip", ".tif")
        url = f"zip+{url}!{input_file_name}"
    else:
        input_file_name = os.path.basename(parsed_url.path)

    # output_file_name = input_file_name.replace(".asc", ".tif")
    output_file_name = input_file_name

    # create a COG profile to set compression on the output file
    dst_profile = cog_profiles.get("deflate")

    # Create the COG in-memory and save to S3
    try:
        with rasterio.open(url) as input_image:
            with MemoryFile() as output_image:
                cog_translate(input_image, output_image.name, dst_profile, in_memory=True, nodata=-9999)

                logger.info(f"\t - {input_file_name} downloaded & converted to COG: {datetime.now() - start_time}")
                start_time = datetime.now()

                # DEBUGGING
                if debug:
                    with open(os.path.join(output_path, output_file_name), "wb") as f:
                        f.write(output_image.read())
                        output_image.seek(0)

                    logger.info(f"\t - {output_file_name} saved locally: {datetime.now() - start_time}")
                    start_time = datetime.now()

                # upload to AWS S3
                aws_response = s3_client.upload_fileobj(output_image, s3_bucket, s3_path, Config=s3_config)

                if aws_response is not None:
                    logger.warning(f"\t - {output_file_name} copy to S3 problem : {aws_response}")
                else:
                    logger.info(f"\t - {output_file_name} uploaded to S3: {datetime.now() - start_time}")

    except Exception as ex:
        logger.warning(f"\t - {output_file_name} - convert failed : {ex}")


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
