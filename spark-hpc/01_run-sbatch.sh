#!/bin/bash

cp spark-job-TEMPLATE.sh spark-job.sh

sed -i "s#CURRENT_DIR#${PWD}#g" spark-job.sh
sbatch spark-job.sh

rm -rf spark-job.sh
