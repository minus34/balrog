#!/usr/bin/env bash

sudo chown -R ec2-user:ec2-user /data

sudo mkdir -p /data/tmp
cd /data/tmp
curl -O https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/5m-dem/national_utm_mosaics/nationalz56_ag.zip --progress-bar
unzip -o nationalz56_ag.zip
rm nationalz56_ag.zip
cd ~






#
## nope!
#aws s3 sync s3://elevation-direct-downloads/5m-dem/national_utm_mosaics s3://bushfire-rasters/geoscience_australia/5m-dem/national_utm_mosaics/