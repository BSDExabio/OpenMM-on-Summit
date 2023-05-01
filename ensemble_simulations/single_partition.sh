#!/bin/bash
#
#BSUB -P YOURACCOUNTHERE
#BSUB -W 0:40
#BSUB -nnodes 1500
#BSUB -alloc_flags "gpudefault nvme"
#BSUB -J dask_testing
#BSUB -o omm_md.%J.out
#BSUB -e omm_md.%J.err
#BSUB -N
#BSUB -B

date

# set up the modules and python environment
module load cuda/11.0.3 gcc/11.1.0
module unload xalt

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

# set active directory and file variables
SRC_DIR=/path/to/source/dir
RUN_DIR=/path/to/working/dir/$LSB_JOBID
SCHEDULER_FILE=${RUN_DIR}/scheduler_file.json

# prepare the run directory
if [ ! -d "$RUN_DIR" ]
then
    mkdir -p $RUN_DIR
fi
cd $RUN_DIR

# Copy over the hosts allocated for this job so that we can later verify that all the allocated nodes were busy with the correct worker allocation.
cat $LSB_DJOB_HOSTFILE | sort | uniq > $LSB_JOBID.hosts		# catches both the batch and compute nodes; not super interested in the batch node though, right?

# We need to figure out the number of nodes to later spawn the workers
N_HOSTS=$(cat $LSB_JOBID.hosts | wc -l)	# count number of lines in $LSB_JOBID.hosts; one line will be associated with the batch/head node which will not be used to run calculations
let x=$N_HOSTS y=1 N_NODES=x-y
let x=$N_NODES y=6 N_WORKERS=x*y
# arbitrarily setting number of tasks to be 3 x N_WORKERS
let x=$N_WORKERS y=3 N_TASKS=x*y

echo "################################################################################"
echo "Using python: " `which python3`
echo "PYTHONPATH: " $PYTHONPATH
echo "SRC_DIR: " $SRC_DIR
echo "scheduler file:" $SCHEDULER_FILE
echo "NUMBER OF NODES: $N_NODES"
echo "NUMBER OF WORKERS: $N_WORKERS"
echo "NUMBER OF SIMULATION TASKS: $N_TASKS"
echo "################################################################################"

# gathering process ids for each step of the workflow.
dask_pids=""

##
## Start dask scheduler on an arbitrary couple of CPUs (more than one CPU to handle overhead of managing all the dask workers).
##
# The scheduler doesn't need GPUs. We give it 36 CPUs to handle the overhead of managing so many workers.
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE &
dask_pids="$dask_pids $!"

##
## Start the dask-workers, which will be paired up to an individual GPU.  This bash script will manage the dask workers and GPU allocation for each Summit node.
##
# Now launch ALL the dask workers simultaneously.  They won't come up at the same time, though.
jsrun --smpiargs="off" --rs_per_host 6 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 1 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_worker.stdout --stdio_stderr ${RUN_DIR}/dask_worker.stderr \
	dask-worker --nthreads 1 --nworkers 1 --interface ib0 --no-dashboard --no-nanny --reconnect --scheduler-file ${SCHEDULER_FILE} &
dask_pids="$dask_pids $!"

# Run the client task manager; this just needs a single core to noodle away on but we can give it some more just in case...
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr.stdout --stdio_stderr ${RUN_DIR}/tskmgr.stderr \
	python3 ${SRC_DIR}/md_tskmgr.py --scheduler-file $SCHEDULER_FILE \
					--N-simulations $N_TASKS \
					--timings-file timings.csv \
					--tskmgr-log-name tskmgr.log \
					--working-dir ${RUN_DIR} \
					--run-dir md_simulations \
					--nvme-path /mnt/bb/$USER/
signal=$?

# We're done so kill the scheduler and worker processes
for pid in $dask_pids
do
        kill $pid
done

[ $signal -eq 0 ] && echo "Run finished successfully."

date

