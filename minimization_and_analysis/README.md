
# Energy Minimization and Analysis
This repository holds the code to run a multi-stepped Dask workflow to prepare AlphaFold predicted models for energy minimization and post-processing analyses. 
The first two steps of the workflow follow very closely the steps performed in the last stage of the original AlphaFold algorithm, where predicted models are "fixed" and run through iterations of OpenMM energy minimization.
Since the implementation of AlphaFold on Summit does not inherently perform this energy minimization step, we needed to recreate it externally from the AlphaFold workflow. 

Looking beyond the application for structural modeling of proteins, this workflow is a good example of how to implement a perfectly parallel, yet complex workflow on Summit that can scale to leadership compute allocations.
Not only does the workflow consist of three tasks per protein model but the types of tasks are performed more efficiently on different compute resources.  
Specifically, there are three steps to the Dask workflow presented here: 
1. Preprocessing of models to prepare them for MD simulation. Specifically, "fix" each protein structure by adding missing atoms (mainly hydrogens) that are absent in the starting pdb file. This task is efficiently performed on a single core of CPU resources. 
2. A single iteration of OpenMM energy minimization, where the model has been parameterized using a user-defined force field and a user-defined selection of atoms are restrained using a harmonic energy potential (with a user-defined spring constant). By default, OpenMM performs energy minimization calculations indefinitely until an energy convergence criteria is reached. The final structure from this calculation is saved. Molecular dynamics modeling methods are efficiently performed on GPU resources, so this type of task is provided one CPU core as well as one GPU. 
3. Postprocessing of the energy-minimized structure. In this case, a very basic CoG-translation calculation is performed to center of the structure's CoG at the origin. This is not all that interesting and is only used to demonstrate the ability of the dask workflow to daisy-chain tasks together. This specific calculation is efficiently performed on a single core of CPU resources. 

Through the use of Dask.distributed functionality, these tasks are easily coupled together and efficiently distributed to the specified compute resources.

## The batch script: `dask_workflow.sh`
Submission of the batch script on Summit is easy:

```
bsub dask_workflow.sh
```

There are a few important details to note in this code. 
Firstly, the `dask-scheduler` call is given only 2 CPU cores (see line 75, `--cpu_per_rs`). 
As the number of tasks being performed during the workflow or the total number of workers become large, you will want to increase the compute resources provided to the Dask scheduler because it handles the communications between all workers and the Dask client.
Secondly, two populations of Dask workers are initiated on lines 87 and 93. 
The first population is provided 1 CPU core and 1 GPU; this set of workers are labeled "GPU=1". On Summit, there are 6 of these workers per compute node since each node has 6 GPUs. 
The second population is provided 1 CPU core; this set of workers are labeled "CPU=1". 32 of these workers are started per compute node. 
Finally, the tasks are defined, the Dask client is started, and results are gathered within the `energy_minimization_workflow.py` script. 

## The client script: `energy_minimization_workflow.py`
Each of the three tasks described above are defined as functions within this script. 
As a whole, these functions represent one, over-arching pipeline for a single protein model broken down into three seperate tasks. 
Doing so allows Dask to efficiently manage compute resources and task assignment to workers; only Step 2 requires GPU resources so the other steps are pushed to CPU-only workers. 
The general structure of the dask workflow is independent of the task functions defined in this script; accounting for different task dependencies and resource requirements, this Dask workflow can be implemented for any number of other applications with minimal changes.
The dask Client is instantiated on line 466, which connects the client with scheduler. 
Then, tasks are mapped to the client, leading to the scheduler beginning to hand out tasks to the workers, on lines 486 to 491. 
Note that the list of Dask "futures" is the input for the next steps' set of tasks, thus the tasks are daisy-chained together; once a step 1 task is completed, the associated step 2 task is ready to be processed by a worker and so on. 
Finally, the set of "future" objects are iterated over as they complete to gather timing information and perform any final logging steps (there are none in this example code). 
Once all tasks are completed, the client script shuts down and the batch script kills the jsrun'ed scheduler and worker commands. 

