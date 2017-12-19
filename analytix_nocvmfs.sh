#!/bin/bash

#scl enable python27 bash

export PATH=/afs/cern.ch/user/p/pkothuri/.local/bin:$PATH
export HIVE_CONF_DIR=/etc/hive/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export SPARK_CONF_DIR=/etc/spark/conf
export HADOOP_HOME=/usr/lib/hadoop
export SPARK_HOME=/afs/cern.ch/user/p/pkothuri/public/spark-2.1.0-bin-hadoop2.6
export PATH=/afs/cern.ch/user/p/pkothuri/public/spark-2.1.0-bin-hadoop2.6/bin:$PATH
export PYTHONPATH=/afs/cern.ch/user/p/pkothuri/.local/lib/python2.7/site-packages
export PYSPARK_DRIVER_PYTHON=jupyter-notebook
export PYSPARK_DRIVER_PYTHON_OPTS="--ip=`hostname` --browser='/dev/null' --port=8888"
