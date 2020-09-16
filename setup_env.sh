#!/bin/bash
set -euo pipefail

# if this is set then the script will download and unzip a local instance of spark.
export DOWNLOAD_SPARK=

# create a new virtual environment, activate it and install all the requirements
python3 -m venv spark-workshop-env
source ./spark-workshop-env/bin/activate
pip install -r requirements.txt


echo "Please run the following 2 statements in your console:"
echo "
export PYSPARK_PYTHON="$PWD/spark-workshop-env/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/spark-workshop-env/bin/python"
"

# handle Spark installation
if [ -n "${DOWNLOAD_SPARK}" ]
then
  export SPARK_VERSION=2.4.6
  export HADOOP_VERSION=2.7
  export SPARK_FNAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

  # Spark installation is ~220MB
  curl -O https://www.apache.si/spark/spark-$SPARK_VERSION/$SPARK_FNAME
  tar xzf ./$SPARK_FNAME
fi


