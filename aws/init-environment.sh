#!/bin/bash

#
# Copyright 2013 AMALTHEA REU; Dillon Rose; Michel Rouly
#

set -e

download_location=/home/hadoop/downloads
libraries=/home/hadoop/lib

bucket=$1
path=$2
filename=$(echo $path | sed 's/.*\///g')

mkdir -p $download_location
mkdir -p $libraries

wget -T 10 -t 5 http://$bucket.s3.amazonaws.com/$path -O $download_location/$filename
tar -xvf $download_location/$filename -C $libraries

for f in $libraries/*
do
	echo "HADOOP_CLASSPATH=\"$f:$HADOOP_CLASSPATH\"" >> /home/hadoop/conf/hadoop-user-env.sh
done
