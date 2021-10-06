#!/usr/bin/env bash

sudo chown -R ec2-user:ec2-user /data

sudo mkdir -p /data/tmp
cd /data
curl -O https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/5m-dem/national_utm_mosaics/nationalz56_ag.zip --progress-bar
cd tmp
unzip /data/nationalz56_ag.zip





#
## nope!
#aws s3 sync s3://elevation-direct-downloads/5m-dem/national_utm_mosaics s3://bushfire-rasters/geoscience_australia/5m-dem/national_utm_mosaics/