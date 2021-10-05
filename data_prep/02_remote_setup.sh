#!/usr/bin/env bash

# installs Python packages to enable converting images to Cloud Optimised GeoTIFFs (COGs)

PYTHON_VERSION="3.9"
NVIDIA_DRIVER_VERSION="470.57.02"  # CUDA 11.4

# check if proxy server required
while getopts ":p:" opt; do
  case $opt in
  p)
    PROXY=$OPTARG
    ;;
  esac
done

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

echo "-------------------------------------------------------------------------"
echo " Creating new Conda Environment 'cog'"
echo "-------------------------------------------------------------------------"

# Create Conda environment
echo "y" | conda create -n cog python=${PYTHON_VERSION}

# activate and setup env
conda activate cog
conda config --env --add channels conda-forge
conda config --env --set channel_priority strict

# reactivate for changes to take effect
conda activate cog

echo "-------------------------------------------------------------------------"
echo " Installing Python packages"
echo "-------------------------------------------------------------------------"

echo "y" | conda install -c conda-forge rasterio[s3] rio-cogeo fiona requests boto3
