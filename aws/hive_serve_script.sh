#!/bin/bash

#
# Copyright 2013 AMALTHEA REU; Dillon Rose; Michel Rouly
#

mkdir -p /home/hadoop/hiveserver/
touch /home/hadoop/hiveserver/myhiveserver.log
hive --service hiveserver # &> /home/hadoop/hiveserver/myhiveserver.log