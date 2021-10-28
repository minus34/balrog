
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
    debug = True

    ram_to_use = 8

    output_path = os.path.join(pathlib.Path.home(), "tmp/bushfire/veg")

    input_list = [{"name": "30m_land_cover",
                   "input_path": os.path.join(pathlib.Path.home(), "Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 30M JUNE 2021/Standard"),
                   "glob_pattern": "_SURFACECOVER_30M_",
                   "output_file": os.path.join(output_path, "geoscape_30m_land_cover.tif"),
                   "s3_file_path": "geoscape/geoscape_30m_land_cover.tif"},
                  {"name": "trees",
                   "input_path": os.path.join(pathlib.Path.home(), "Downloads/Trees_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Trees/Trees JUNE 2021/Standard"),
                   "glob_pattern": "_TREES_",
                   "output_file": os.path.join(output_path, "geoscape_trees.tif"),
                   "s3_file_path": "geoscape/geoscape_trees.tif"},
                  {"name": "trees_metadata",
                   "input_path": os.path.join(pathlib.Path.home(), "Downloads/Trees_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Trees/Trees JUNE 2021/Standard"),
                   "glob_pattern": "_TREES_METADATA_",
                   "output_file": os.path.join(output_path, "geoscape_trees_metadata.tif"),
                   "s3_file_path": "geoscape/geoscape_trees_metadata.tif"},
                  {"name": "2m_land_cover",
                   "input_path": os.path.join(pathlib.Path.home(), "Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 2M JUNE 2021/Standard"),
                   "glob_pattern": "_SURFACECOVER_2M_",
                   "output_file": os.path.join(output_path, "geoscape_2m_land_cover.tif"),
                   "s3_file_path": "geoscape/geoscape_2m_land_cover.tif"}]
else:
    debug = False

    ram_to_use = 480

    output_path = "/data/geoscape"

    input_list = [{"name": "trees",
                   "input_path": "/data/geoscape/Trees/Trees JUNE 2021/Standard",
                   "glob_pattern": "_TREES_",
                   "output_file": os.path.join(output_path, "geoscape_trees.tif"),
                   "s3_file_path": "geoscape/geoscape_trees.tif"},
                  {"name": "trees_metadata",
                   "input_path": "/data/geoscape/Trees/Trees JUNE 2021/Standard",
                   "glob_pattern": "_TREES_METADATA_",
                   "output_file": os.path.join(output_path, "geoscape_trees_metadata.tif"),
                   "s3_file_path": "geoscape/geoscape_trees_metadata.tif"},
                  {"name": "30m land cover",
                   "input_path": "/data/geoscape/Surface Cover/Surface Cover 30M JUNE 2021/Standard",
                   "glob_pattern": "_SURFACECOVER_30M_",
                   "output_file": os.path.join(output_path, "geoscape_30m_land_cover.tif"),
                   "s3_file_path": "geoscape/geoscape_30m_land_cover.tif"},
                  {"name": "2m land cover",
                   "input_path": "/data/geoscape/Surface Cover/Surface Cover 2M JUNE 2021/Standard",
                   "glob_pattern": "_SURFACECOVER_2M_",
                   "output_file": os.path.join(output_path, "geoscape_2m_land_cover.tif"),
                   "s3_file_path": "geoscape/geoscape_2m_land_cover.tif"}
                  ]

if debug:
    mga_zones = [49, 51, 52]
else:
    mga_zones = range(49, 57)

# how many parallel processes to run
max_processes = multiprocessing.cpu_count()

# set max RAM usage (divide by 4 as there are 4 processes - one per dataset)
gdal.SetCacheMax(int(ram_to_use / 4) * 1024 * 1024)


def main():
    full_start_time = datetime.now()
    logger.info(f"START mosaic and transform images : {full_start_time}")

    mp_pool = multiprocessing.Pool(max_processes)
    mp_results = mp_pool.map_async(process_dataset, input_list)

    results = mp_results.get()
    mp_pool.close()
    mp_pool.join()

    for result in results:
        logger.info(f" - {result}")

    logger.info(f"FINISHED mosaic and transform images : {datetime.now() - full_start_time}")


def process_dataset(input_dict):
    """process 1 dataset at a time using parallel processing"""
    full_start_time = datetime.now()
    start_time = datetime.now()

    files_to_mosaic = list()

    # mosaic and transform to WGS84 lat/long for each MGA zone (aka UTM South zones on GDA94 datum)
    for zone in mga_zones:
        files = glob.glob(os.path.join(input_dict["input_path"], f"*{input_dict['glob_pattern']}Z{zone}*.tif"))
        num_images = len(files)

        if num_images > 0:
            files_to_mosaic.extend(files)
        else:
            print(f" - {input_dict['name']} : zone {zone} has no images")

    # mosaic all merged files and output as a single Cloud Optimised GeoTIFF (COG) in GDA94 lat/long for all of AU
    if len(files_to_mosaic) > 0:
        gdt = gdal.Warp(input_dict["output_file"], files_to_mosaic, format="COG", options="-overwrite -multi -wm 80% -t_srs EPSG:4283 -co TILED=YES -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")
        del gdt

        print(f" - {input_dict['name']} done : {datetime.now() - start_time}")
        start_time = datetime.now()

        # upload to AWS S3 if not debugging
        if not debug:
            try:
                aws_response = s3_client.upload_file(input_dict["output_file"], s3_bucket, input_dict["s3_file_path"], Config=s3_config)
                print(f"- {input_dict['name']} : image uploaded to s3 : {datetime.now() - start_time}")
            except:
                print(f"- {input_dict['name']} : FAILED - image upload to s3 : AWS token probably expired : {datetime.now() - start_time}")
    else:
        print(f" - {input_dict['name']} : no files to merge")

    return f"{input_dict['name']} done : {datetime.now() - full_start_time}"


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
