#/bin/bash

find logs -mindepth 1 -not -name '.gitignore' -type f -exec rm -rf {} +
find workers -mindepth 1 -not -name '.gitignore' -type f -exec rm -rf {} +
find logs -mindepth 1 -not -name '.gitignore' -type d -exec rm -rf {} +
find workers -mindepth 1 -not -name '.gitignore' -type d -exec rm -rf {} +
rm -rf spark-job.sh
