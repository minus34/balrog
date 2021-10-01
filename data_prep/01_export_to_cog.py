
import glob
# import json
import io
import logging
import math
import multiprocessing
import os
import numpy
import pathlib
import platform
import psycopg2
import psycopg2.extras
import rasterio.crs
import rasterio.mask
import requests
import sys
import time
import zipfile

from datetime import datetime
from rasterio.io import MemoryFile
from rio_cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# ------------------------------------------------------------------------------------------------------------------
# START: edit settings
# ------------------------------------------------------------------------------------------------------------------


output_path = os.path.join(script_dir, "output")

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

    # create temp output directory
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    # DEBUGGING
    url = "https://portal.spatial.nsw.gov.au/download/dem/56/Sydney-DEM-AHD_56_5m.zip"

    # download zip file and get list of compressed files
    files = list(download_extract_zip(url))

    crs = None
    raster_bytes = None
    input_file = None
    output_file = None

    # get the raster and it's coordinate system
    for file in files:
        file_path = os.path.join(output_path, file[0])
        file_obj = file[1]

        # get raster as a byte array
        if file_path.endswith(".asc"):
            raster_bytes = file_obj
            # input_file = file_path
            output_file = file_path.replace(".asc", ".tif")

        # get well known text coordinate system
        if file_path.endswith(".prj"):
            proj_string = file_obj.decode("utf-8")
            crs = rasterio.crs.CRS.from_wkt(proj_string)

        # # save file to disk
        # with open(file_path, "wb") as f:
        #     f.write(file_obj)

    logger.info(f"\t - File unzipped & saved to memory : {datetime.now() - start_time}")
    start_time = datetime.now()

    # create COG profile and add coordinate system
    dst_profile = cog_profiles.get("deflate")
    dst_profile.update({"crs": str(crs)})

    # Create the COG in-memory
    with MemoryFile(raster_bytes) as mem_src:
        with mem_src.open() as dataset:
            with MemoryFile() as mem_dst:
                cog_translate(
                    dataset,
                    output_file,
                    # mem_dst.name,
                    dst_profile,
                    in_memory=False,
                    nodata=-9999,
                )

                mem_dst.write(output_file)

                # print(str(mem_dst.crs))
                #
                # print(len(mem_dst))





    # if input_file is not None:
    #     with rasterio.open(input_file) as dataset:

    # if raster_bytes is not None and crs is not None:
    #     # kwargs = {'crs': str(crs)}
    #
    #     # with open(raster_bytes, 'rb') as f, MemoryFile(f) as memfile:
    #
    #     with MemoryFile(raster_bytes) as memfile:
    #         with memfile.open() as dataset:
                # raster = src.read()
                # kwargs = src.meta.copy()
                # kwargs.update({
                #     'crs': str(crs)
                # })
                #
                # with MemoryFile(raster, **kwargs) as memfile2:

                # dataset.crs = crs

                # data_array = dataset.read()
                #
                # print(str(dataset.crs))
                # print(f"file size is {len(data_array)}")

    logger.info(f"\t - Raster dataset created : {datetime.now() - start_time}")
    start_time = datetime.now()







    # if input_file is not None:
    #     with rasterio.open(input_file) as raster:
    #         raster_metadata = raster.meta.copy()





    # in_data = f"/vsigzip//vsicurl/{url}"
    #
    # with MemoryFile() as mem_dst:
    #     # Creating the COG, with a memory cache and no download. Shiny.
    #     cog_translate(
    #         in_data,
    #         mem_dst.name,
    #         cog_profiles.get("deflate"),
    #         in_memory=True,
    #         nodata=-9999,
    #     )
    #
    #     print(len(mem_dst))





    # with rasterio.open(f"_{output_type}") as dataset:
    #     slope=dataset.read(1)
    # return slope


    # # test download of GIC DTM tiles
    # latitude = -34.024461
    # longitude = 151.051168
    # zoom = 16
    # dtm_tile = download_gic_dtm(latitude, longitude, zoom)
    #
    # with open(os.path.join(output_path, "test_dtm_tile.tif"), "wb") as f:
    #     f.write(bytes(dtm_tile))

    # # Test processing of DEM to get slope and aspect
    # for file_path in glob.glob(input_path):
    #     # process_dem(file_path, "hillshade")
    #     # process_dem(file_path, "color-relief")
    #     process_dem(file_path, "slope")
    #     process_dem(file_path, "aspect")
    #
    #     print(f"Processed : {os.path.basename(file_path)}")

    #     slope = calculate_slope(dem_file_path)
    #     aspect = calculate_aspect(dem_file_path)

    # print(type(slope))
    # print(slope.dtype)
    # print(slope.shape)


    logger.info(f"FINISHED : Export to COG : {datetime.now() - full_start_time}")


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
