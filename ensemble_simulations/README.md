## Running an ensemble of perfectly-parallel simulations using a Dask workflow and OpenMM
The HPC resources on Summit and other compute machines can be utilized to run massive amounts of simulation in a perfectly parallel execution model. 
The code provided here is a generalizable example to run such a workflow. 

Specifically, the `md_tskmgr.py` python script functions as a Dask Client script that initiates all of the tasks to be performed in the workflow (in this case, OpenMM simulations). 
The provided batch scripts (i.e. `single_partition.sh`) are used to submit the workflow to a Summit compute allocation and spin up the Dask distributed Scheduler and a set of Workers. 
The Scheduler handles the coordination of tasks between the Client script and the Workers.
The Workers are subsets or portions of the full compute allocation that function as the resources used to perform individual tasks.
The `analyze_timings.py` and `plotting_timing_results.ipynb` are basic python scripts/jupyter notebooks that I have developed to visualize the workflow's efficiency. 


To submit the workflow on Summit: 
```
bsub single_partition.sh
```


