
import glob
import pathlib
import os

input_path = os.path.join(pathlib.Path.home(), "Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 2M JUNE 2021/Standard")
output_file = os.path.join(pathlib.Path.home(), "tmp/bushfire/veg/geoscape_trees.tif")

# get file names only
file_list = [os.path.basename(file_name) for file_name in glob.glob(os.path.join(input_path, "*.tif"))]
files_string = " ".join(file_list)

command1 = f"gdal_merge.py -o temp.tif -of GTiff -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS {files_string}\n"
command2 = f"gdal_translate.py temp.tif {output_file} -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS\n"

# out commands to bash file
with open("xx_trees_merge_images.sh", "w") as f:
    f.write("#!/usr/bin/env bash\n\n")
    f.write("conda activate geo\n\n")
    f.write(f"cd '{input_path}'\n\n")
    f.write(command1)
    f.write(command2)
    f.write("rm temp.tif")
    f.write(f"aws s3 cp {output_file} s3://bushfire-rasters/geoscape/")
