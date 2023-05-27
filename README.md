# OpenMM-on-Summit

This repo is meant as a tutorial on how to set up and run OpenMM at scale on OLCF Summit.
Installation and setup information is directly forked from the inspiremd git repo on the same subject that can be found [here](https://github.com/inspiremd/conda-recipes-summit).
The subdirectories have code associated with applying the OpenMM toolkit and Dask.distributed workflows to perform molecular simulation tasks on leadship-scale compute allocations, prioritizing the full, efficient utilization of GPU resources while still putting the excess CPU resources to good use. 
[comment](Admittedly, the given simulation tasks are relatively bog-standard and boring.) 
In [ensemble_simulations](https://github.com/BSDExabio/OpenMM-on-Summit/tree/main/ensemble_simulations), a large set of basic NVT simulations are run as tasks with each worker having 1 GPU : 1 CPU.
In [minimization_and_analysis](https://github.com/BSDExabio/OpenMM-on-Summit/tree/main/minimization_and_analysis), a list of structures are provided in the Dask client script and each is subsequently parameterized, energy minimized, and quickly analyzed.  

## Setting up OpenMM on Summit

### Setup

First, install `miniconda`:
```bash
module load cuda/11.0.3 gcc/11.1.0
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-ppc64le.sh
bash Miniconda3-latest-Linux-ppc64le.sh -b -p miniconda
# Initialize your ~/.bash_profile
miniconda/bin/conda init bash
source ~/.bashrc
```

The `ppc64le` packages have been uploaded to the [`omnia`](https://anaconda.org/omnia) and [`conda-forge`](https://anaconda.org/conda-forge) channels:
```bash
# Add conda-forge and omnia to your channel list
conda config --add channels omnia --add channels conda-forge
# Update to conda-forge versions of packages
conda update --yes --all
```
### Installing OpenMM

```bash
# Create a new environment named 'openmm'
conda create -n openmm python==3.9
# Activate it
conda activate openmm
# Install the 'openmm' 7.6.0 for ppc64le into this environment
conda install --yes -c omnia-dev/label/cuda110 openmm
conda install pdbfixer dask parmed
```

## Testing OpenMM

```bash
# start an interactive job on a single node of SUMMIT
bsub -W 2:00 -nnodes 1 -P bip198 -alloc_flags gpudefault -Is /bin/bash

# Source the bashrc. Load the CUDA and appropriate MPI modules:
source ~/.bashrc
module load cuda/11.0.3 gcc/11.1.0

# Activate the OpenMM conda environment
conda activate openmm

# Run the OpenMM-provided installation test script; already in path.
python3 -m openmm.testInstallation

# Should return something like this:

OpenMM Version: 7.6
Git Revision: ad113a0cb37991a2de67a08026cf3b91616bafbe

There are 4 Platforms available:

1 Reference - Successfully computed forces
2 CPU - Successfully computed forces
3 CUDA - Successfully computed forces
4 OpenCL - Error computing forces with OpenCL platform

OpenCL platform error: Error compiling kernel: 

Median difference in forces between platforms:

Reference vs. CPU: 6.3174e-06
Reference vs. CUDA: 6.73126e-06
CPU vs. CUDA: 6.57691e-07

All differences are within tolerance.
```
We are not concerned about the OpenCL platform so this error can be ignored. 
You should check that the differences in forces between the reference and your desired platform are small (e-06 or less).

## Further testing of OpenMM

```
# Need to move into the examples directory of a git-cloned OpenMM repository. 
git clone https://github.com/openmm/openmm.git
cd openmm-master/examples/

# Run the benchmark via jsrun. 
# one resource set (-n 1), one MPI process (-a 1), one core (-c 1), one GPU (-g 1)
jsrun --smpiargs="none" -n 1 -a 1 -c 1 -g 1 python3 benchmark.py --platform=CUDA --test=pme --precision=mixed --seconds=30 --heavy-hydrogens

# Should return something like this: 

Platform: CUDA
Precision: mixed

Test: pme (cutoff=0.9)
Ensemble: NVT
Step Size: 5 fs
Integrated 50367 steps in 29.286 seconds
742.968 ns/day
```

## Running a Single OpenMM MD Simulation
A basic MD simulation script is presented in the `run_nvt_simulations.py` that will load up a Amber formatted prmtop and inpcrd (rst7) files and initiate a MD simulation. 
This script can be run as below. 
It will produce a log file, a simulation metric data file, and a DCD trajectory file as well as two forms of restart files, an OpenMM state file and an OpenMM checkpoint file. 
The current parameters only run a 0.5 ns trajectory of a NVT simulation but these can be changed to run a variety of other simulations.

```
python3 run_nvt_simulations.py {prmtop} {rst7} {output_dir_descriptor} ./
```

## Running OpenMM simulations at scale on Summit
See the `ensemble_simulations/` and `minimization_and_analysis/`  subdirectories for codes and discussion of running OpenMM simulations across many nodes of Summit. 

