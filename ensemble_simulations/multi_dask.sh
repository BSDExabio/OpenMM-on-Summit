#!/bin/bash
#
##BSUB -P BIF135
#BSUB -W 0:45
#BSUB -nnodes 2000
#BSUB -alloc_flags gpudefault
#BSUB -J openmm_testing
#BSUB -o md.%J.out
#BSUB -e md.%J.err
#BSUB -N
#BSUB -B

date

# set active directory and file variables
SRC_DIR=/gpfs/alpine/proj-shared/bip198/dask_testing/md_simulations
RUN_DIR=$SRC_DIR/quad_dask_2000_nodes
SCHEDULER_FILE1=${RUN_DIR}/scheduler_file_1.json
SCHEDULER_FILE2=${RUN_DIR}/scheduler_file_2.json
SCHEDULER_FILE3=${RUN_DIR}/scheduler_file_3.json
SCHEDULER_FILE4=${RUN_DIR}/scheduler_file_4.json

N_SIMS=9000	# 9000 node scheduler/worker/client setup

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
echo $NUM_NODES
let x=$NUM_NODES y=-1 NUM_NODES=x+y
echo $NUM_NODES
let x=$NUM_NODES y=4 split_nodes=x/y
let x=$split_nodes y=6 num_workers=x*y
#export NUM_NODES=$(expr $NUM_NODES - 1)		# subtract by one to ignore the batch node
#split_nodes=$(echo $NUM_NODES/2)		# integer math

echo "################################################################################"
echo "Using python: " `which python3`
echo "PYTHONPATH: " $PYTHONPATH
echo "SRC_DIR: " $SRC_DIR
echo "scheduler file1:" $SCHEDULER_FILE1
echo "scheduler file2:" $SCHEDULER_FILE2
echo "scheduler file3:" $SCHEDULER_FILE3
echo "scheduler file4:" $SCHEDULER_FILE4
echo "NUM_NODES: $NUM_NODES"
echo "split_nodes: $split_nodes"
echo "num_workers: $num_workers"
echo "################################################################################"

##
## Start dask scheduler on an arbitrary couple of CPUs (more than one CPU to handle overhead of managing all the dask workers).
##

# The scheduler doesn't need GPUs. We give it 36 CPUs to handle the overhead of managing so many workers.
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler1.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler1.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE1 &

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler2.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler2.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE2 &

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler3.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler3.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE3 &

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 36 --gpu_per_rs 0 --latency_priority cpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/dask_scheduler4.stdout --stdio_stderr ${RUN_DIR}/dask_scheduler4.stderr \
	dask-scheduler --interface ib0 --no-dashboard --no-show --scheduler-file $SCHEDULER_FILE4 &

# Give the scheduler a chance to spin up.
sleep 5

##
## Start the dask-workers, which will be paired up to an individual GPU.  This bash script will manage the dask workers and GPU allocation for each Summit node.
##

## Now launch ALL the dask workers simultaneously.  They won't come up at the same time, though.
# Using spawn_workers.sh to explicitly spin up workers on host nodes...

jsrun --smpiargs="off" --nrs $split_nodes --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 6 --gpu_per_rs 6 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/spawn_workers1.stdout --stdio_stderr ${RUN_DIR}/spawn_workers1.stderr \
	$SRC_DIR/spawn_workers.sh $RUN_DIR $SCHEDULER_FILE1 &

jsrun --smpiargs="off" --nrs $split_nodes --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 6 --gpu_per_rs 6 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/spawn_workers2.stdout --stdio_stderr ${RUN_DIR}/spawn_workers2.stderr \
	$SRC_DIR/spawn_workers.sh $RUN_DIR $SCHEDULER_FILE2 &

jsrun --smpiargs="off" --nrs $split_nodes --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 6 --gpu_per_rs 6 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/spawn_workers3.stdout --stdio_stderr ${RUN_DIR}/spawn_workers3.stderr \
	$SRC_DIR/spawn_workers.sh $RUN_DIR $SCHEDULER_FILE3 &

jsrun --smpiargs="off" --nrs $split_nodes --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 6 --gpu_per_rs 6 --latency_priority gpu-cpu --bind none \
	--stdio_stdout ${RUN_DIR}/spawn_workers4.stdout --stdio_stderr ${RUN_DIR}/spawn_workers4.stderr \
	$SRC_DIR/spawn_workers.sh $RUN_DIR $SCHEDULER_FILE4 &

echo Waiting for workers

# Hopefully long enough for some workers to spin up and wait for work
sleep 5

pids=""

# Run the client task manager; like the scheduler, this just needs a single core to noodle away on, which python takes naturally (no jsrun call needed)
jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr1.stdout --stdio_stderr ${RUN_DIR}/tskmgr1.stderr \
	python3 ${SRC_DIR}/dask_tskmgr.py --scheduler-file $SCHEDULER_FILE1 --N-simulations $N_SIMS --timings-file ${RUN_DIR}/timings1.csv --working-dir ${RUN_DIR} --script-path ${SRC_DIR}/run_nvt_simulations.py --tskmgr-log-name tskmgr1.log --run-dir run1 & 
pids="$pids $!"

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr2.stdout --stdio_stderr ${RUN_DIR}/tskmgr2.stderr \
	python3 ${SRC_DIR}/dask_tskmgr.py --scheduler-file $SCHEDULER_FILE2 --N-simulations $N_SIMS --timings-file ${RUN_DIR}/timings2.csv --working-dir ${RUN_DIR} --script-path ${SRC_DIR}/run_nvt_simulations.py --tskmgr-log-name tskmgr2.log --run-dir run2 & 
pids="$pids $!"

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr3.stdout --stdio_stderr ${RUN_DIR}/tskmgr3.stderr \
	python3 ${SRC_DIR}/dask_tskmgr.py --scheduler-file $SCHEDULER_FILE3 --N-simulations $N_SIMS --timings-file ${RUN_DIR}/timings3.csv --working-dir ${RUN_DIR} --script-path ${SRC_DIR}/run_nvt_simulations.py --tskmgr-log-name tskmgr3.log --run-dir run3 & 
pids="$pids $!"

jsrun --smpiargs="off" --nrs 1 --rs_per_host 1 --tasks_per_rs 1 --cpu_per_rs 1 --gpu_per_rs 0 --latency_priority cpu-cpu \
	--stdio_stdout ${RUN_DIR}/tskmgr4.stdout --stdio_stderr ${RUN_DIR}/tskmgr4.stderr \
	python3 ${SRC_DIR}/dask_tskmgr.py --scheduler-file $SCHEDULER_FILE4 --N-simulations $N_SIMS --timings-file ${RUN_DIR}/timings4.csv --working-dir ${RUN_DIR} --script-path ${SRC_DIR}/run_nvt_simulations.py --tskmgr-log-name tskmgr4.log --run-dir run4 & 
pids="$pids $!"

echo $pids

wait $pids

# We're done so kill the scheduler and worker processes
jskill all

echo Run finished.

date

#rm ${RUN_DIR}/enam_726/*/*stt
#rm ${RUN_DIR}/enam_726/*/*chkpt

