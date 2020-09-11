#!/bin/bash

cp spark-job.sh spark-job-temp.sh

sed -i "s#CURRENT_DIR#${PWD}#g" spark-job-temp.sh
sbatch spark-job-temp.sh

rm -rf spark-job-temp.sh
