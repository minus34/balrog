
import glob
import pathlib
import os

input_path = os.path.join(pathlib.Path.home(), "Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 2M JUNE 2021/Standard")
output_file = os.path.join(pathlib.Path.home(), "tmp/bushfire/veg/geoscape_2m_land_cover.tif")

# output commands to bash file
with open("xx_trees_merge_images.sh", "w") as f:
    f.write("#!/usr/bin/env bash\n\n")
    f.write("conda activate geo\n\n")
    f.write(f"cd '{input_path}'\n\n")

    warped_files = list()

    # get merge & warp commands for each projection (MGA zones, aka UTM South zones on GDA94 datum)
    for zone in range(49, 50):
        file_list = [os.path.basename(file_name) for file_name in glob.glob(os.path.join(input_path, f"*_Z{zone}_*.tif"))]
        files_string = " ".join(file_list)

        first_file = f"temp_Z{zone}.tif"
        second_file = f"temp_Z{zone}_4326.tif"

        f.write(f"echo 'Processing MGA Zone {zone}'\n")
        f.write(f"gdal_merge.py -o {first_file} -of GTiff -n 0 -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS {files_string}\n")
        f.write(f"gdalwarp -t_srs EPSG:4326 -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS -overwrite {first_file} {second_file}\n")
        f.write(f"rm {first_file}\n\n")

        warped_files.append(second_file)
        warped_files_string = " ".join(warped_files)

    f.write(f"echo 'Processing AU'\n")
    f.write(f"gdal_merge.py -o temp_au.tif -of GTiff -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS {warped_files_string}\n")
    f.write(f"gdal_translate temp_au.tif {output_file} -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS\n")
    f.write("rm temp_au.tif\n")
    f.write(f"aws s3 cp {output_file} s3://bushfire-rasters/geoscape/\n")
