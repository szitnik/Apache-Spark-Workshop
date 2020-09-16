#/bin/bash

find logs -mindepth 1 -not -name '.gitignore' -type f,d -exec rm -rf {} +
find workers -mindepth 1 -not -name '.gitignore' -type f,d -exec rm -rf {} +
rm -rf slurm*.out
rm -rf spark-job.sh
