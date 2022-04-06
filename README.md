# OpenMM-on-Summit

This repo is just meant as a tutorial for my group on how to set up openmm on summit.
Most of the material is taken directly from the inspiremd git repo on the same subject that can be found [here](https://github.com/inspiremd/conda-recipes-summit).
Also, examples provided are taken from the openmm git repo that can be found [here](https://github.com/openmm/openmm).

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
# test installation of OpenMM
python -m simtk.testInstallation

# Source the bashrc. Load the CUDA and appropriate MPI modules:
source ~/.bashrc
module load cuda/11.0.3 gcc/11.1.0

# Make sure to activate conda environment
conda activate openmm

# Run a simtk provided script testing the installation.
python -m simtk.testInstallation

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

```
# FURTHER TESTING OF THE INSTALLATION
# Need to move into the examples directory of a git cloned openmm-master. 
git clone https://github.com/openmm/openmm.git
cd openmm-master/examples/

# Run the benchmark via jsrun requesting
# one resource set (-n 1), one MPI process (-a 1), one core (-c 1), one GPU (-g 1)
jsrun --smpiargs="none" -n 1 -a 1 -c 1 -g 1 python benchmark.py --platform=CUDA --test=pme --precision=mixed --seconds=30 --heavy-hydrogens
```
I see the following benchmarks on Summit:
```
Platform: CUDA
Precision: mixed

Test: pme (cutoff=0.9)
Ensemble: NVT
Step Size: 5 fs
Integrated 50367 steps in 29.286 seconds
742.968 ns/day
```



