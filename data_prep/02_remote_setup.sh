#!/usr/bin/env bash

# installs Python packages to enable converting images to Cloud Optimised GeoTIFFs (COGs)

#PYTHON_VERSION="3.9"

# required to keep long running sessions active
sudo yum install -y tmux

# check if proxy server required
while getopts ":p:" opt; do
  case $opt in
  p)
    PROXY=$OPTARG
    ;;
  esac
done

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

# Install Conda to create a Python 3.9 environment (AWS yum repos stop at Python 3.7)
echo "-------------------------------------------------------------------------"
echo " Installing Conda"
echo "-------------------------------------------------------------------------"

# download & install silently
curl -fSsl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
sh Miniconda3-latest-Linux-x86_64.sh -b

# initialise Conda & reload bash environment
${HOME}/miniconda3/bin/conda init
source ${HOME}/.bashrc

# update Python packages
echo "y" | conda update conda

#echo "-------------------------------------------------------------------------"
#echo " Creating new Conda Environment 'cog'"
#echo "-------------------------------------------------------------------------"
#
## Create Conda environment
#echo "y" | conda create -n cog python=${PYTHON_VERSION}
#
## activate and setup env
#conda activate cog
#conda config --env --add channels conda-forge
#conda config --env --set channel_priority strict
#
## reactivate for changes to take effect
#conda activate cog

echo "-------------------------------------------------------------------------"
echo " Installing Python packages"
echo "-------------------------------------------------------------------------"

echo "y" | conda install -c conda-forge gdal rasterio[s3] rio-cogeo psycopg2 postgis requests boto3

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
echo " Mount storage"
echo "-------------------------------------------------------------------------"

sudo mkfs -t xfs /dev/nvme1n1
sudo mkdir /data
sudo mount /dev/nvme1n1 /data

sudo chown -R ec2-user:ec2-user /data
mkdir -p /data/tmp/cog

echo "-------------------------------------------------------------------------"
echo " Setup Postgres Database"
echo "-------------------------------------------------------------------------"

# GNAF table
aws s3 cp s3://bushfire-rasters/geoscape/gnaf-sydney.dmp ${HOME} --no-progress --quiet
echo "Postgres dump file copied"

# start postgres
initdb -D postgres
pg_ctl -D postgres -l logfile start

# create new database
createdb --owner=ec2-user geo

# add PostGIS extension to database, create schema and tables
psql -d geo -f ${HOME}/02_create_tables.sql

# restore GNAF table (ignore the ALTER TABLE error)
pg_restore -Fc -d geo -p 5432 -U ec2-user ${HOME}/gnaf-sydney.dmp


#cd /data/tmp
#curl -O https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/5m-dem/national_utm_mosaics/nationalz56_ag.zip --progress-bar
#unzip -o nationalz56_ag.zip
#rm nationalz56_ag.zip

echo "-------------------------------------------------------------------------"
echo " Copy elevation data from S3"
echo "-------------------------------------------------------------------------"

# copy elevation files from S3
aws s3 sync s3://bushfire-rasters/geoscience_australia/1sec-dem /data/tmp/cog/
#aws s3 sync s3://bushfire-rasters/nsw_dcs_spatial_services/ /data/tmp/cog/