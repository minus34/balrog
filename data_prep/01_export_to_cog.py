
import boto3
import io
import logging
import multiprocessing
import os
import pathlib
import rasterio.crs
import rasterio.mask
import requests
import zipfile

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from rasterio.io import MemoryFile
from rio_cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------

debug = True

output_path = os.path.join(script_dir, "output")

s3_bucket = "bushfire-rasters"
s3_path = "nsw_dcs_spatial_services"

# ------------------------------------------------------------------------------------------------------------------
# END: edit settings
# ------------------------------------------------------------------------------------------------------------------

# how many parallel processes to run (only used for downloading images, hence can use 2x CPUs safely)
max_processes = multiprocessing.cpu_count()
max_postgres_connections = max_processes + 1


def main():
    full_start_time = datetime.now()
    start_time = datetime.now()

    logger.info(f"START : Export to COG : {full_start_time}")

    # create debug output directory
    if debug:
        pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    # DEBUGGING
    url = "https://portal.spatial.nsw.gov.au/download/dem/56/Sydney-DEM-AHD_56_5m.zip"

    # download zip file and get list of compressed files
    file_list = list(download_extract_zip(url))

    # get the raster image and it's coordinate system
    image, crs, output_file_name = get_raster_and_crs(file_list, debug)

    logger.info(f"\t - File unzipped & saved to memory : {datetime.now() - start_time}")
    start_time = datetime.now()

    # convert to COG image
    cog_image = convert_to_cog(image, crs, output_file_name, debug)
    # cog_image = convert_to_cog(image, crs, output_file_name)

    logger.info(f"\t - Raster dataset created : {datetime.now() - start_time}")
    start_time = datetime.now()

    # upload to AWS S3
    s3_file_path = f"{s3_path}/dem/{output_file_name}"
    s3_client = boto3.client("s3")
    config = TransferConfig(multipart_threshold=10240 ** 2)  # 10MB
    aws_response = s3_client.upload_fileobj(cog_image, s3_bucket, s3_file_path, Config=config)

    if aws_response is not None:
        print("\t\t - WARNING: {} copy to S3 problem : {}".format(output_file_name, aws_response))

    logger.info(f"\t - Image uploaded to S3 : {datetime.now() - start_time}")

    logger.info(f"FINISHED : Export to COG : {datetime.now() - full_start_time}")


def get_raster_and_crs(file_list, debug=False):
    image = None
    crs = None
    output_file_name = None

    # get the raster and it's coordinate system
    for file in file_list:
        file_name = file[0]
        file_obj = file[1]

        # get raster as an in-memory file
        if file_name.endswith(".asc"):
            image = MemoryFile(file_obj)
            output_file_name = file_name.replace(".asc", ".tif")

        # get well known text coordinate system
        if file_name.endswith(".prj"):
            proj_string = file_obj.decode("utf-8")
            crs = rasterio.crs.CRS.from_wkt(proj_string)

        if debug:
            with open(os.path.join(output_path, file_name), "wb") as f:
                f.write(file_obj)

    return image, crs, output_file_name


def convert_to_cog(input_image, crs, output_file_name, debug=False):
    """Takes a raster file & it's coordinate system and outputs a cloud optimised tiff (COG) image"""

    # create COG profile and add coordinate system
    dst_profile = cog_profiles.get("deflate")
    dst_profile.update({"crs": str(crs)})

    # Create the COG in-memory
    dataset = input_image.open()
    output_image = MemoryFile()

    # give the image a name
    # if output_file_name is not None:
    image_name = output_file_name.replace(".tif","")
    # else:
    #     image_name = output_image.name

    cog_translate(dataset, image_name, dst_profile, in_memory=True, nodata=-9999)

    # DEBUGGING
    if debug:
        with open(os.path.join(output_path, output_file_name), "wb") as f:
            f.write(output_image.read())

    return output_image


def download_extract_zip(url: str):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile.read()



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
