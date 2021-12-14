#!/usr/bin/env bash

# installs Python packages to enable converting images to Cloud Optimised GeoTIFFs (COGs)

PYTHON_VERSION="3.9"

# required to keep long running sessions active
sudo yum install -y tmux mdadm

# check if proxy server required
if [ -n "$1" ]
  then
    PROXY=$1
fi

if [ -n "${PROXY}" ];
  then
    export no_proxy="localhost,127.0.0.1,:11";
    export http_proxy="$PROXY";
    export https_proxy=${http_proxy};
    export HTTP_PROXY=${http_proxy};
    export HTTPS_PROXY=${http_proxy};
    export NO_PROXY=${no_proxy};

    echo "-------------------------------------------------------------------------";
    echo " Proxy is set to ${http_proxy}";
    echo "-------------------------------------------------------------------------";
fi

echo "-------------------------------------------------------------------------"
echo " create RAID O array from local SSDs and mount storage"
echo "-------------------------------------------------------------------------"

# create RAID 0 array - WARNING: assumes EC2 instance has 4 SSDs)
sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=4 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1
# format it
sudo mkfs -t xfs /dev/md0
# mount it to a directory
sudo mkdir /data
sudo mount /dev/md0 /data
# set permissions
sudo chown -R ec2-user:ec2-user /data
# save RAID settings in case of reboot
sudo mdadm --detail --scan > ~/mdadm.conf
sudo cp ~/mdadm.conf /etc/mdadm.conf
## add config to system
#sudo update-initramfs -u

## move system temp folder to RAID 0 array
#mkdir -p /data/tmp

# Install Conda to create a Python 3.9 environment (AWS yum repos stop at Python 3.7)
echo "-------------------------------------------------------------------------"
echo " Installing Conda"
echo "-------------------------------------------------------------------------"

# download & install silently
curl -fSsl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
sh Miniconda3-latest-Linux-x86_64.sh -b -p /data/miniconda3

# initialise Conda & reload bash environment
/data/miniconda3/bin/conda init
source ${HOME}/.bashrc

echo "-------------------------------------------------------------------------"
#echo "Creating new Conda Environment 'geo'"
echo "Install Python packages"
echo "-------------------------------------------------------------------------"

## deactivate current env
#conda deactivate

# update Conda platform
conda update -y conda

## Create Conda environment on the RAID 0 array (Postgres logs fill up / otherwise)
#conda create -y -p /data/conda/geo python=${PYTHON_VERSION}
#
## activate and setup env
#conda activate /data/conda/geo
#conda config --env --add channels conda-forge
#conda config --env --set channel_priority strict

## reactivate for env vars to take effect
#conda activate /data/conda/geo

# install lots of geo packages -- note: Shapely 1.8.0. install createa a strange environment issue that can freeze Python scripts
conda install -y -c conda-forge gdal rasterio[s3] rio-cogeo psycopg2 postgis shapely=1.7.1 fiona requests boto3

# remove proxy if set
if [ -n "${PROXY}" ];
  then
    unset http_proxy
    unset HTTP_PROXY
    unset https_proxy
    unset HTTPS_PROXY
    unset no_proxy
    unset NO_PROXY

    echo "-------------------------------------------------------------------------"
    echo " Proxy removed"
    echo "-------------------------------------------------------------------------"
    echo ""
fi

echo "-------------------------------------------------------------------------"
echo " Setup Postgres Database"
echo "-------------------------------------------------------------------------"

# start postgres
initdb -D postgres
pg_ctl -D postgres -l logfile start

# increase memory usage and minimise logging (don't care if it crashes and we lose everything)
psql -d postgres -c "ALTER SYSTEM SET max_parallel_workers = 64;"
psql -d postgres -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 64;"
psql -d postgres -c "ALTER SYSTEM SET shared_buffers = '128GB';"
psql -d postgres -c "ALTER SYSTEM SET wal_buffers = '1GB';"
psql -d postgres -c "ALTER SYSTEM SET max_wal_size = '64GB';"
psql -d postgres -c "ALTER SYSTEM SET wal_level = 'minimal';"
psql -d postgres -c "ALTER SYSTEM SET max_wal_senders = 0;"
psql -d postgres -c "ALTER SYSTEM SET archive_mode = 'off';"
psql -d postgres -c "ALTER SYSTEM SET fsync = 'off';"
psql -d postgres -c "ALTER SYSTEM SET full_page_writes = 'off';"
psql -d postgres -c "ALTER SYSTEM SET synchronous_commit = 'off';"
psql -d postgres -c "ALTER SYSTEM SET autovacuum = 'off';"

# shut down server
pg_ctl -D postgres stop

# copy WAL files to big drive, symlink to the new folder and delete the old folder (to free up space on /)
mkdir -p /data/postgres
cp -a ~/postgres/pg_wal/. /data/postgres/pg_wal
rm -r ~/postgres/pg_wal
ln -s /data/postgres/pg_wal ~/postgres/pg_wal

# restart down server
pg_ctl -D postgres start

# create new database on mounted drive (not enough space on default drive)
psql -d postgres -c "CREATE TABLESPACE dataspace OWNER \"ec2-user\" LOCATION '/data/postgres';"
createdb --owner=ec2-user geo -D dataspace

# add PostGIS and create schema
psql -d geo -c "create extension if not exists postgis;"
psql -d geo -c "create schema if not exists bushfire;alter schema bushfire owner to \"ec2-user\";"

# restore buildings table(s) (ignore the ALTER TABLE errors)
psql -d geo -c "create schema if not exists geoscape_202111;alter schema geoscape_202111 owner to \"ec2-user\";"
aws s3 cp s3://bushfire-rasters/geoscape/202111/geoscape.dmp /data/
pg_restore -Fc -d geo -p 5432 -U ec2-user /data/geoscape.dmp --clean

## restore vegetation table(s) (ignore the ALTER TABLE errors)
#aws s3 cp s3://bushfire-rasters/vegetation/nvis6/nvis6.dmp /data/
#pg_restore -Fc -d geo -p 5432 -U ec2-user /data/nvis6.dmp --clean

# restore GNAF table(s) (ignore the ALTER TABLE errors)
aws s3 cp s3://bushfire-rasters/geoscape/gnaf.dmp /data/
pg_restore -Fc -d geo -p 5432 -U ec2-user /data/gnaf.dmp --clean

echo "-------------------------------------------------------------------------"
echo " Copy elevation data from S3"
echo "-------------------------------------------------------------------------"

mkdir -p /data/dem/geotiff
mkdir /data/dem/cog

# copy Geoscape rasters
aws s3 sync s3://bushfire-rasters/geoscape/202111 /data/ --exclude "*" --include "*.tif"

# copy elevation files from S3
#aws s3 sync s3://bushfire-rasters/geoscience_australia/1sec-dem /data/dem/ --exclude "*" --include "*.tif"
#aws s3 cp s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_dem_s.tif /data/dem/geotiff/
#aws s3 cp s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_aspect.tif /data/dem/geotiff/
#aws s3 cp s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_slope.tif /data/dem/geotiff/

echo "-------------------------------------------------------------------------"
echo " Remote setup finished!"
echo "-------------------------------------------------------------------------"
