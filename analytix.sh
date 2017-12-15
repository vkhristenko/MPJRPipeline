#!/bin/bash

# cvmfs for all of the libs/packages/etc...
#source  /cvmfs/sft.cern.ch/lcg/views/LCG_88/x86_64-centos7-gcc62-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/views/LCG_88/x86_64-slc6-gcc49-opt/setup.sh

# hadoop configuration / spark configuration / yarn / etc...
echo "Using Cluster: $1"
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh $1

# To use jupyter 
export PYSPARK_DRIVER_PYTHON=jupyter-notebook
export PYSPARK_DRIVER_PYTHON_OPTS="--ip=`hostname` --browser='/dev/null' --port=8888"

# good to run spark
