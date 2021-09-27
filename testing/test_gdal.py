
import glob
import os
# import numpy as np
# import rasterio

from osgeo import gdal

# the directory of this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# input dem files path
input_path = os.path.join(script_dir, "input", "*.asc")
output_path = os.path.join(script_dir, "output")


def process_dem(dem_file, output_type):
    output_file = os.path.join(output_path, output_type,
                               os.path.basename(file_path).replace(".asc", f"_{output_type}.tif"))

    if output_type == "color-relief":
        gdal.DEMProcessing(output_file, dem_file, output_type,
                           colorFilename=os.path.join(script_dir, "colour_palette.txt"))
    else:
        gdal.DEMProcessing(output_file, dem_file, output_type)

    # with rasterio.open(f"_{output_type}") as dataset:
    #     slope=dataset.read(1)
    # return slope


for file_path in glob.glob(input_path):
    # process_dem(file_path, "hillshade")
    # process_dem(file_path, "color-relief")
    process_dem(file_path, "slope")
    process_dem(file_path, "aspect")

    print(f"Processed : {os.path.basename(file_path)}")

#     slope = calculate_slope(dem_file_path)
#     aspect = calculate_aspect(dem_file_path)

# print(type(slope))
# print(slope.dtype)
# print(slope.shape)
