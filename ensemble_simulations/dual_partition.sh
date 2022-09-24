#!/bin/bash
#
#BSUB -P BIF135-ONE
#BSUB -W 0:45
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
SRC_DIR=/gpfs/alpine/bif135/proj-shared/rbd_work/dask_testing/md_simulations/nvme_capable
RUN_DIR=$SRC_DIR/$LSB_JOBID
SCHEDULER_FILE1=${RUN_DIR}/scheduler_file_1.json
SCHEDULER_FILE2=${RUN_DIR}/scheduler_file_2.json

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
let x=$N_NODES y=2 split_nodes=x/y
let x=$split_nodes y=6 N_WORKERS=x*y
let x=$N_WORKERS y=3 N_TASKS=x*y

echo "################################################################################"
echo "Using python: " `which python3`
echo "PYTHONPATH: " $PYTHONPATH
echo "SRC_DIR: " $SRC_DIR
echo "scheduler file1:" $SCHEDULER_FILE1
echo "scheduler file2:" $SCHEDULER_FILE2
echo "NUMBER OF NODES: $N_NODES"
echo "NUMBER OF PARTITIONS: 2"
echo "NUMBER OF NODES PER PARTITION: $split_nodes"
echo "NUMBER OF WORKERS PER PARTITION: $N_WORKERS"
echo "NUMBER OF SIMULATION TASKS PER PARTITION: $N_TASKS"
echo "################################################################################"

# gathering process ids for each step of the workflow.
dask_pids=""

##
## Start dask scheduler on an arbitrary couple of CPUs (more than one CPU to handle overhead of managing all the dask workers).
##
# Schedulers don't need GPUs. We give each 36 CPUs to handle the overhead of managing so many workers.
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler1.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler1.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE1 &
dask_pids="$dask_pids $!"

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler2.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler2.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE2 &
dask_pids="$dask_pids $!"

##
## Start the dask-workers, which will be paired up to an individual GPU.  This bash script will manage the dask workers and GPU allocation for each Summit node.
##
# Now launch ALL the dask workers simultaneously.  They won't come up at the same time, though.
jsrun --smpiargs="off" --nrs $N_WORKERS --rs_per_host 6 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 1 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_worker1.stdout --stdio_stderr ${RUN_DIR}/dask_worker1.stderr \
	dask-worker --nthreads 1 --nworkers 1 --interface ib0 --no-dashboard --no-nanny --reconnect --scheduler-file ${SCHEDULER_FILE1} &
dask_pids="$dask_pids $!"

jsrun --smpiargs="off" --nrs $N_WORKERS --rs_per_host 6 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 1 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_worker2.stdout --stdio_stderr ${RUN_DIR}/dask_worker2.stderr \
	dask-worker --nthreads 1 --nworkers 1 --interface ib0 --no-dashboard --no-nanny --reconnect --scheduler-file ${SCHEDULER_FILE2} &
dask_pids="$dask_pids $!"

client_pids=""
# Run the client task manager; this just needs a single core to noodle away on but we can give it some more just in case...
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr1.stdout --stdio_stderr ${RUN_DIR}/tskmgr1.stderr \
	python3 ${SRC_DIR}/md_tskmgr.py --scheduler-file $SCHEDULER_FILE1 --N-simulations $N_TASKS --timings-file timings1.csv --tskmgr-log-name tskmgr1.log --working-dir ${RUN_DIR} --run-dir md_simulations1 --nvme-path /mnt/bb/$USER/ &
client_pids="$client_pids $!"
signal1=$?

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr2.stdout --stdio_stderr ${RUN_DIR}/tskmgr2.stderr \
	python3 ${SRC_DIR}/md_tskmgr.py --scheduler-file $SCHEDULER_FILE2 --N-simulations $N_TASKS --timings-file timings2.csv --tskmgr-log-name tskmgr2.log --working-dir ${RUN_DIR} --run-dir md_simulations2 --nvme-path /mnt/bb/$USER/ & 
client_pids="$client_pids $!"
signal2=$?

wait $client_pids

# We're done so kill the scheduler and worker processes
for pid in $dask_pids
do
        kill $pid
done

[ $signal1 -eq 0 ] && echo "Partition 1 finished successfully."
[ $signal2 -eq 0 ] && echo "Partition 2 finished successfully."

date

