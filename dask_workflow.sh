#!/bin/bash
#
# Batch submission script for testing post-AF minimization run on Summit
#
# Support issue-14 branch on PSP git repository
#
##BSUB -P BIP198
#BSUB -P BIF135
#BSUB -W 2:00
#BSUB -nnodes 32
#BSUB -alloc_flags gpudefault
#BSUB -J af_min
#BSUB -o afold_min.%J.out
#BSUB -e afold_min.%J.err
#BSUB -N
#BSUB -B

date

# set active directory and file variables
SRC_DIR=/gpfs/alpine/proj-shared/bip198/dask_testing/md_simulations
RUN_DIR=$SRC_DIR/test1
SCHEDULER_FILE=${RUN_DIR}/scheduler_file.json

N_SIMS=6

# set up the modules and python environment
module load cuda/11.0.3 gcc/11.1.0

# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/ccs/home/davidsonrb/Apps/miniconda_SUMMIT_2/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/ccs/home/davidsonrb/Apps/miniconda_SUMMIT_2/etc/profile.d/conda.sh" ]; then
        . "/ccs/home/davidsonrb/Apps/miniconda_SUMMIT_2/etc/profile.d/conda.sh"
    else
        export PATH="/ccs/home/davidsonrb/Apps/miniconda_SUMMIT_2/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

# activate the conda environment
conda activate openmm

# prepare the run directory
if [ ! -d "$SCHEDULER_DIR" ]
then
    mkdir -p $RUN_DIR
fi
cd $RUN_DIR

# Copy over the hosts allocated for this job so that we can later verify that all the allocated nodes were busy with the correct worker allocation.
cat $LSB_DJOB_HOSTFILE | sort | uniq > $LSB_JOBID.hosts		# catches both the batch and compute nodes; not super interested in the batch node though, right?

# We need to figure out the number of nodes to later spawn the workers
NUM_NODES=$(cat $LSB_JOBID.hosts | wc -l)	# count number of lines in $LSB_JOBID.hosts
export NUM_NODES=$(expr $NUM_NODES - 1)		# subtract by one to ignore the batch node

echo "################################################################################"
echo "Using python: " `which python3`
echo "PYTHONPATH: " $PYTHONPATH
echo "SRC_DIR: " $SRC_DIR
echo "scheduler file:" $SCHEDULER_FILE
echo "NUM_NODES: $NUM_NODES"
echo "################################################################################"

##
## Start dask scheduler on an arbitrary couple of CPUs (more than one CPU to handle overhead of managing all the dask workers).
##

# The scheduler doesn't need GPUs. We give it 2 CPUs to handle the overhead of managing so many workers.
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 2 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE &

# Give the scheduler a chance to spin up.
sleep 15

##
## Start the dask-workers, which will be paired up to an individual GPU.  This bash script will manage the dask workers and GPU allocation for each Summit node.
##

# Now launch ALL the dask workers simultaneously.  They won't come up at the same time, though.
jsrun --smpiargs="off" --rs_per_host 6 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 1 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_worker.stdout --stdio_stderr ${RUN_DIR}/dask_worker.stderr \
	dask-worker --nthreads 1 --nprocs 1 --interface ib0 --no-dashboard --no-nanny --reconnect --scheduler-file ${SCHEDULER_FILE} &

echo Waiting for workers

# Hopefully long enough for some workers to spin up and wait for work
sleep 30

# Run the client task manager; like the scheduler, this just needs a single core to noodle away on, which python takes naturally (no jsrun call needed)
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr.stdout --stdio_stderr ${RUN_DIR}/tskmgr.stderr \
	python3 ${SRC_DIR}/dask_tskmgr.py --scheduler-file $SCHEDULER_FILE --N-simulations $N_SIMS --timings-file ${RUN_DIR}/timings.csv --working-dir ${RUN_DIR} --script-path ${SRC_DIR}/run_nvt_simulations.py

# We're done so kill the scheduler and worker processes
jskill all

echo Run finished.

date
