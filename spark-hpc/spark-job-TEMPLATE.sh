#!/bin/bash
#SBATCH -N 1
#SBATCH -t 00:10:00
#SBATCH --ntasks-per-node 3
#SBATCH --cpus-per-task 2
#SBATCH --output=logs/slurm_stdout_err__%j.log

module purge
module load Spark/2.4.0-Hadoop-2.7-Java-1.8

# MAIN DIR
export JOB_SPARK_DIR="CURRENT_DIR"

export JOB_SPARK_LOG_DIR="${JOB_SPARK_DIR}/logs"
export JOB_SPARK_WORKER_DIR="${JOB_SPARK_DIR}/workers"
export SPARK_CONF_DIR="${JOB_SPARK_DIR}/conf"

# Start Master
start-master.sh

# Start Workers
start-slave.sh spark://`hostname`:7077

# Spark cluster running to submit jobs
# via driver from login node
#sleep infinity

# Run driver on an HPC node
spark-submit --master spark://`hostname`:7077 "${JOB_SPARK_DIR}/job.py"
