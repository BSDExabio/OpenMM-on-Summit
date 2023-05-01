#!/bin/bash
#
#BSUB -P YOURACCOUNTHERE
#BSUB -W 2:00
#BSUB -nnodes 1
#BSUB -alloc_flags gpudefault
#BSUB -J af_min
#BSUB -o out.%J
#BSUB -e err.%J
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
PYTHONPATH=/gpfs/alpine/bif135/proj-shared/rbd_work/dask_testing/Min_and_analysis:$PYTHONPATH

# set active directories
SRC_DIR=/path/to/source/dir
RUN_DIR=/path/to/working/dir/$LSB_JOBID
SCHEDULER_FILE=${RUN_DIR}/scheduler_file.json

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
let x=$N_NODES y=6 GPU_N_WORKERS=x*y
let x=$N_NODES y=32 CPU_N_WORKERS=x*y

echo "################################################################################"
echo "Using python: " `which python3`
echo "PYTHONPATH: " $PYTHONPATH
echo "SRC_DIR: " $SRC_DIR
echo "SCHEDULER FILE: " $SCHEDULER_FILE
echo "NUMBER OF NODES: " $N_NODES
echo "NUMBER OF GPU WORKERS: " $GPU_N_WORKERS
echo "NUMBER OF CPU WORKERS: " $CPU_N_WORKERS
echo "################################################################################"

# gathering process ids for each step of the workflow.
dask_pids=""

##
## Start dask schedulers on an arbitrary couple of CPUs (more than one CPU to handle overhead of managing all the dask workers).
##
# dask scheduler
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 2 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/gpu_dask_scheduler.stdout --stdio_stderr ${RUN_DIR}/gpu_dask_scheduler.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE &
dask_pids="$dask_pids $!"

##
## Start the dask-worker sets
##
# dask worker set 1 for GPU resources
jsrun --smpiargs="off" --rs_per_host 6 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 1 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/gpu_dask_worker.stdout --stdio_stderr ${RUN_DIR}/gpu_dask_worker.stderr \
	dask-worker --nthreads 1 --nworkers 1 --interface ib0 --no-dashboard --no-nanny --reconnect --scheduler-file ${SCHEDULER_FILE} --resources "GPU=1" &
dask_pids="$dask_pids $!"

# dask worker set 2 for CPU resources
jsrun --smpiargs="off" --rs_per_host 32 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/cpu_dask_worker.stdout --stdio_stderr ${RUN_DIR}/cpu_dask_worker.stderr \
	dask-worker --nthreads 1 --nworkers 1 --interface ib0 --no-dashboard --no-nanny --reconnect --scheduler-file ${SCHEDULER_FILE} --resources "CPU=1" &
dask_pids="$dask_pids $!"

# Run the client task manager; like the scheduler, this just needs a single core to noodle away on
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 0 --latency_priority cpu-cpu \
	python3 ${SRC_DIR}/energy_minimization_workflow.py --scheduler-file $SCHEDULER_FILE\
       							   --list-file ${SRC_DIR}/structure_list.txt \
							   --output-dir ${RUN_DIR}/ \
							   --parameter-file ${SRC_DIR}/sample_openmm_parameters.pkl \
							   --tskmgr-log-name ${RUN_DIR}/tskmgr.log \
							   --timings-file ${RUN_DIR}/timings.csv

signal=$?

# shutting down dask scheduler and worker commands
for pid in $dask_pids
do
        kill $pid
done

[ $signal -eq 0 ] && echo "Run finished successfully."

date

