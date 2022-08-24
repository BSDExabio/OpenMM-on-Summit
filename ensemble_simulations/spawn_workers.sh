#!/usr/bin/env bash
#
# Start up the dask workers with associated GPUSs for a single
# Summit node.  This is called from an LSF batch script and is for a single
# resource set.
#

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

RUN_DIR=$1		# Where we will run this script and expect output
SCHEDULER_FILE=$2	# dask file for scheduler and workers to find each other

echo "spawn worker python3:" $(which python3)

# Grab the device numbers for all the local GPUs
gpus=$(nvidia-smi --list-gpus | cut -c5)

for gpu in $gpus; do
    env CUDA_VISIBLE_DEVICES=$gpu \
     dask-worker --nthreads 1 --nworkers 1 --interface ib0 \
      --no-dashboard --no-nanny --reconnect --scheduler-file ${SCHEDULER_FILE} &
done

wait

