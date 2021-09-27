
from osgeo import gdal
import numpy as np
import rasterio

dem_file_path = "/Users/s57405/Downloads/NSW Government - Spatial Services/DEM/1 Metre/PortHacking202004-LID1-AHD_3186234_56_0002_0002_1m/PortHacking202004-LID1-AHD_3186234_56_0002_0002_1m.asc"


def calculate_slope(DEM):
    gdal.DEMProcessing('slope.tif', DEM, 'slope')
    with rasterio.open('slope.tif') as dataset:
        slope=dataset.read(1)
    return slope


def calculate_aspect(DEM):
    gdal.DEMProcessing('aspect.tif', DEM, 'aspect')
    with rasterio.open('aspect.tif') as dataset:
        aspect=dataset.read(1)
    return aspect


slope=calculate_slope(dem_file_path)
aspect=calculate_aspect(dem_file_path)

print(type(slope))
print(slope.dtype)
print(slope.shape)