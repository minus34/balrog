


# WARNING - currently doesn't work
# - it flattens the 2m & 30m land cover colour ramps
# - it adds false values to the tree heights (band 1)

import boto3
import glob
import logging
import multiprocessing
import os
import pathlib
import platform
import rasterio

from boto3.s3.transfer import TransferConfig
from contextlib import contextmanager
from datetime import datetime
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject, Resampling

# setup connection to AWS S3
s3_client = boto3.client("s3")
s3_config = TransferConfig(multipart_threshold=10240 ** 2)  # 20MB

s3_bucket = "bushfire-rasters"

if platform.system() == "Darwin":
    debug = True

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
                   "s3_file_path": "geoscape/geoscape_trees_metadata.tif"}
                  # {"name": "30m land cover",
                  #  "input_path": "/data/geoscape/Surface Cover/Surface Cover 30M JUNE 2021/Standard",
                  #  "glob_pattern": "_SURFACECOVER_30M_",
                  #  "output_file": os.path.join(output_path, "geoscape_30m_land_cover.tif"),
                  #  "s3_file_path": "geoscape/geoscape_30m_land_cover.tif"},
                  # {"name": "2m land cover",
                  #  "input_path": "/data/geoscape/Surface Cover/Surface Cover 2M JUNE 2021/Standard",
                  #  "glob_pattern": "_SURFACECOVER_2M_",
                  #  "output_file": os.path.join(output_path, "geoscape_2m_land_cover.tif"),
                  #  "s3_file_path": "geoscape/geoscape_2m_land_cover.tif"}
                  ]

if debug:
    mga_zones = range(49, 50)
else:
    mga_zones = range(49, 57)

target_crs = "EPSG:4283"

# how many parallel processes to run
max_processes = multiprocessing.cpu_count()


def main():
    full_start_time = datetime.now()
    logger.info(f"START mosaic and transform images : {full_start_time}")

    mp_pool = multiprocessing.Pool(max_processes)
    mp_results = mp_pool.map_async(process_dataset, input_list)
    # mp_results = mp_pool.map_async(process_dataset, input_list, chunksize=1)  # use map_async to show progress

    # while not mp_results.ready():
    #     print(f"Datasets remaining : {mp_results._number_left} : {datetime.now()}")
    #     sys.stdout.flush()
    #     time.sleep(10)

    # print(f"\r\n", end="")
    results = mp_results.get()
    mp_pool.close()
    mp_pool.join()

    for result in results:
        logger.info(f" - {result}")

    logger.info(f"FINISHED mosaic and transform images : {datetime.now() - full_start_time}")


def process_dataset(input_dict):
    """process 1 dataset at a time using parallel processing"""
    full_start_time = datetime.now()
    warped_files_to_mosaic = list()

    # os.environ["GDAL_CACHEMAX"] = "8000"

    # print(f"START - {input_dict['name']} : mosaic and transform images : {full_start_time}")

    # mosaic and transform to WGS84 lat/long for each MGA zone (aka UTM South zones on GDA94 datum)
    for zone in mga_zones:
        start_time = datetime.now()

        files_to_mosaic = glob.glob(os.path.join(input_dict["input_path"], f"*{input_dict['glob_pattern']}Z{zone}*.tif"))
        num_images = len(files_to_mosaic)

        if num_images > 0:
            interim_file = os.path.join(output_path, f"temp_Z{zone}_{input_dict['name']}.tif")

            # merge images
            loaded_files_to_mosaic = list()

            for file_path in files_to_mosaic:
                src = rasterio.open(file_path, "r")
                loaded_files_to_mosaic.append(src)

            profile = loaded_files_to_mosaic[0].meta.copy()

            # fred = rasterio.band(loaded_files_to_mosaic[0], 1)

            mosaic_array, mosaic_transform = merge(loaded_files_to_mosaic)
            del loaded_files_to_mosaic  # clean up memory

            # create in-memory mosaic image from array and profile
            with get_inmemory_raster(mosaic_array, profile, mosaic_transform) as mosaic:

                # get the transform parameters for reprojection
                transform, width, height = calculate_default_transform(
                    mosaic.crs, target_crs, mosaic.width, mosaic.height, *mosaic.bounds)
                kwargs = profile
                kwargs.update({
                    'crs': target_crs,
                    'transform': transform,
                    'width': width,
                    'height': height
                })

                # reproject to a new file
                with rasterio.open(interim_file, "w", **kwargs) as dst:
                    reproject(
                        source=rasterio.band(mosaic, 1),
                        destination=rasterio.band(dst, 1),
                        src_transform=mosaic.transform,
                        src_crs=mosaic.crs,
                        dst_transform=transform,
                        dst_crs=target_crs,
                        resampling=Resampling.average)

            warped_files_to_mosaic.append(interim_file)

            print(f" - {input_dict['name']} : zone {zone} done ({num_images} images) : {datetime.now() - start_time}")
        else:
            print(f" - {input_dict['name']} : zone {zone} has no images : {datetime.now() - start_time}")

    # mosaic all merged files and output as a single Cloud Optimised GeoTIFF (COG) for all of AU
    start_time = datetime.now()

    if len(warped_files_to_mosaic) > 0:
        # merge images
        loaded_files_to_mosaic = list()

        for file_path in warped_files_to_mosaic:
            src = rasterio.open(file_path, "r")
            loaded_files_to_mosaic.append(src)

        profile = loaded_files_to_mosaic[0].meta.copy()

        mosaic_array, mosaic_transform = merge(loaded_files_to_mosaic)
        del loaded_files_to_mosaic  # clean up memory

        profile.update(
            compress='deflate',
            driver='COG',
            height=mosaic_array.shape[1],
            width=mosaic_array.shape[2],
            transform=mosaic_transform
        )

        with rasterio.open(input_dict["output_file"], "w", **profile) as output_image:
            output_image.write(mosaic_array)

        print(f" - {input_dict['name']} : AU done : {datetime.now() - start_time}")

        # upload to AWS S3 if not debugging
        if not debug:
            try:
                aws_response = s3_client.upload_file(input_dict["output_file"], s3_bucket, input_dict["s3_file_path"], Config=s3_config)
                print(f"\t - {input_dict['name']} : image uploaded to s3 : {datetime.now() - start_time}")
            except:
                print(f"\t - {input_dict['name']} : FAILED - image upload to s3 : AWS token probably expired : {datetime.now() - start_time}")
    else:
        print(f" - {input_dict['name']} : no files to merge : {datetime.now() - start_time}")

    # delete interim files
    for file in warped_files_to_mosaic:
        os.remove(file)

    return f"{input_dict['name']} done : {datetime.now() - full_start_time}"


@contextmanager
def get_inmemory_raster(array, profile, transform):
    profile.update(
        compress='deflate',
        driver='GTiff',
        height=array.shape[1],
        width=array.shape[2],
        transform=transform
    )
    with rasterio.MemoryFile() as memfile:
        with memfile.open(**profile) as dataset: # Open as DatasetWriter
            dataset.write(array)

        with memfile.open() as dataset:  # Reopen as DatasetReader
            yield dataset  # Note yield not return


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
