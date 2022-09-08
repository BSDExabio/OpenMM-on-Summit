#!/usr/bin/env python3
""" Task manager for running the dask pipeline for OpenMM molecular dynamics simulations on Summit. 

    Ingests a set of prmtop and inpcrd files and distributes work to run MD simulations of them among allocated dask workers. The dask workers will be assigned to a single GPU and associated CPUs. 

    USAGE: 
        python3 md_taskmgr.py [-h] --scheduler-file SCHEDULER_FILE --input-file INPUT_FILE --timings-file TIMINGS_FILE.csv --working-dir /path/to/dir/ --script-path /path/to/dir/script.py

    INPUT: 
        -h, --help      show this help message and exit
        --scheduler-file SCHEDULER_FILE, -s SCHEDULER_FILE
                        dask scheduler file
        --N-simulations number_of_simulations, -n number_of_simulations
                        integer used to set the number of total simulations (tasks) that will be performed
        --timings-file TIMINGS_FILE.csv, -ts TIMINGS_FILE.csv 
                        CSV file for task processing timings
        --working-dir /path/to/dir/, -wd /path/to/dir/
                        path that points to the working directory for the output files
        --run-dir /path/to/dir/run_x/, -rd /path/to/dir/run_x/
                        path that points to the dask run's working directory for the output files
        --tskmgr-log-file /path/to/dir/log_file.log, -log /path/to/dir/log_file.log
                        path to a new log file within which logging information will be pritned

    HARD CODED VARIABLES:
        PRMTOP_FILE:
        INPCRD_FILE:
"""

import sys
import time
import argparse
import os
import platform
import stat
import traceback
from pathlib import Path
from uuid import uuid4

import numpy as np
import csv
import openmm
import parmed

import dask.config
from distributed import Client, Worker, as_completed, get_worker

import logging

# NOTE: hard coded variables for the moment.
PRMTOP_FILE = '/gpfs/alpine/bif135/proj-shared/rbd_work/dask_testing/md_simulations/enam_726.prmtop'
INPCRD_FILE = '/gpfs/alpine/bif135/proj-shared/rbd_work/dask_testing/md_simulations/enam_726.ncrst'

#######################################
### BASIC LOGGING FUNCTIONS 
#######################################

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""
    formatter = logging.Formatter('%(asctime)s    %(levelname)s       %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def clean_logger(logger):
    """To cleanup the logger instances once we are done with them"""
    for handle in logger.handlers:
        handle.flush()
        handle.close()
        logger.removeHandler(handle)


#######################################
### DASK RELATED FUNCTIONS
#######################################

def get_num_workers(client):
    """ Get the number of active workers
    :param client: active dask client
    :return: the number of workers registered to the scheduler
    """
    scheduler_info = client.scheduler_info()

    return len(scheduler_info['workers'].keys())


def append_timings(csv_writer, file_object, hostname, worker_id, start_time, stop_time,
                   run_number, n_steps):
    """ append the task timings to the CSV timings file
    :param csv_writer: CSV to which to append timings
    :param hostname: on which the processing took place
    :param worker_id: of the dask worker that did the processing
    :param start_time: start time in *NIX epoch seconds
    :param stop_time: stop time in same units
    :param run_number: that was processed
    :param n_steps: number of steps performed for this run
    """
    csv_writer.writerow({'hostname'  : hostname,
                         'worker_id' : worker_id,
                         'start_time': start_time,
                         'stop_time' : stop_time,
                         'run_number': run_number,
                         'n_steps'   : n_steps})
    file_object.flush()


def submit_pipeline(run_number, prmtop_file = '', inpcrd_file = '', outer_directory = ''):
    """
    """
    start_time = time.time()
    worker = get_worker()

    # setting up the working directory for this specific simulation
    working_directory = Path(outer_directory) / run_number
    try:
        working_directory.mkdir(mode=0o777,parents=True,exist_ok=False)
    except FileExistsError as e:
        #print(f"Exception occurred. Return code {e.returncode}")
        #print(f"Exception cmd: {e.cmd}")

        #print(e.stdout.decode("utf-8"), file=sys.stdout, flush=True)
        #print(e.stderr.decode("utf-8"), file=sys.stderr, flush=True)

        print(str(e), file=sys.stderr, flush=True)
        return platform.node(), worker().id, start_time, time.time(), run_number, -2

    # setting up logging 
    sim_logger = setup_logger(f'sim_logger_{run_number}',str(working_directory / 'simulation.log'))    # unclear to me at this time whether the setup_logger/logging module can take Path objects instead of a string
    sim_logger.info(f'Starting at {time.time()}')

    # move these to a input dictionary that is created before the client script is started
    # openmm parameters:
    time_step   = 2*openmm.unit.femtosecond # simulation timestep
    ts_float    = 2*10**-6		    # femtoseconds to nanosecond
    temperature = 310*openmm.unit.kelvin    # simulation temperature
    friction    = 1/openmm.unit.picosecond  # collision rate
    pressure    = 1.01325*openmm.unit.bar   # simulation pressure 
    mcbarint    = 100			    # number of steps between volume change attempts
    nb_cutoff   = 1.2*openmm.unit.nanometer # distance at which point nonbonding interactions are cutoff
    num_steps   = 250000                    # number of integration steps to run
    trj_freq    = 50000                     # number of steps per written trajectory frame
    data_freq   = 1000                      # number of steps per written simulation statistics
   
    # SETTING up everything
    try:
        # SETTING up the parmed objects
        sim_logger.info('Loading the AMBER files.')
        parmed_system = parmed.load_file(prmtop_file,inpcrd_file)
        sim_logger.info('Creating PARMED system.')
        system = parmed_system.createSystem(nonbondedMethod=openmm.app.PME, nonbondedCutoff=nb_cutoff,constraints=openmm.app.HBonds)
        
        # SETTING up the Langevin dynamics thermostat.
        sim_logger.info('Creating OpenMM integrator.')
        integrator = openmm.LangevinIntegrator(temperature, friction, time_step)
        
        # SETTING the simulation platform .
        sim_logger.info('Choosing OpenMM platform.')
        omm_platform = openmm.Platform.getPlatformByName('CUDA')
        #prop     = dict(CudaPrecision='mixed') # Use mixed single/double precision
        
        # SETTING up an OpenMM simulation.
        sim_logger.info('Creating OpenMM simulation engine.')
        simulation = openmm.app.Simulation(parmed_system.topology, system, integrator, omm_platform)
        
        # SETTING the initial positions.
        sim_logger.info('Setting starting positions.')
        simulation.context.setPositions(parmed_system.positions)
        
        # SETTING the velocities from a Boltzmann distribution at a given temperature.
        sim_logger.info('Setting starting velocities.')
        simulation.context.setVelocitiesToTemperature(temperature)
        
        # SETTING up output files.
        sim_logger.info('Setting reporter files.')
        dcd_file = working_directory / 'traj.dcd'
        simulation.reporters.append(openmm.app.dcdreporter.DCDReporter(str(dcd_file),trj_freq))
        report_file = working_directory / 'traj.out'
        simulation.reporters.append(openmm.app.statedatareporter.StateDataReporter(str(report_file),data_freq,step=True,potentialEnergy=True,kineticEnergy=True,temperature=True,volume=True,density=True,speed=True))
    except Exception as e:
        #print(f"Exception occurred. Return code {e.returncode}")
        #print(f"Exception cmd: {e.cmd}")

        #print(e.stdout.decode("utf-8"), file=sys.stdout, flush=True)
        #print(e.stderr.decode("utf-8"), file=sys.stderr, flush=True)

        print(str(e), file=sys.stderr, flush=True)

        clean_logger(sim_logger)
        return platform.node(), worker().id, start_time, time.time(), run_number, -1

    # RUNNING the simulation
    try:
        sim_logger.info("Starting simulation.")
        sim_start = time.time()
        simulation.step(num_steps)
        sim_end = time.time()
        sim_logger.info(f"Simulation ran for {sim_end-sim_start} seconds. Average speed: {(num_steps*ts_float*86400)/(sim_end-sim_start)} ns day^(-1).")
        sim_logger.info(f'Done! Ending at {time.time()}')

        clean_logger(sim_logger)
        return platform.node(), worker.id, start_time, time.time(), run_number, num_steps

    except Exception as e:
        #print(f"Exception occurred. Return code {e.returncode}")
        #print(f"Exception cmd: {e.cmd}")

        #print(e.stdout.decode("utf-8"), file=sys.stdout, flush=True)
        #print(e.stderr.decode("utf-8"), file=sys.stderr, flush=True)

        print(str(e), file=sys.stderr, flush=True)
        
        nSteps = 0
        if report_file.is_file():
            with open(report_file,'r') as temp:
                final_report = temp.readlines()[-1]
                if final_report[0] != '#':
                    nSteps = int(final_report.split(',')[0])
        
        clean_logger(sim_logger)
        return platform.node(), worker.id, start_time, time.time(), run_number, nSteps


#######################################
### MAIN
#######################################

if __name__ == '__main__':
    # read command line arguments.
    parser = argparse.ArgumentParser(description='Molecular dynamics simulation task manager')
    parser.add_argument('--scheduler-file', '-s', required=True, help='dask scheduler file')
    parser.add_argument('--N-simulations', '-n', required=True, type=int, help='integer used to set the number of total simulations (tasks) that will be performed')
    parser.add_argument('--timings-file', '-ts', required=True, help='CSV file for protein processing timings')
    parser.add_argument('--working-dir', '-wd', required=True, help='path that points to the working directory for the output files')
    parser.add_argument('--run-dir', '-rd', required=True, help="path that points to the dask run's working directory for the output files")
    parser.add_argument('--tskmgr-log-name', '-log', required=True, help='string that will be used to store logging info for this run')
    args = parser.parse_args()

    # first thing: spin up dask client.
    client = Client(scheduler_file=args.scheduler_file,timeout=5000,name='simulationtaskmgr')

    # setting up files and directories
    working_dir = Path(args.working_dir)
    full_working_dir = working_dir / args.run_dir

    # logger file
    main_logger = setup_logger('tskmgr_logger',str(working_dir / f'{args.tskmgr_log_name}'))    # unclear to me at this time whether the setup_logger/logging module can take Path objects instead of a string
    # timing file
    timings_file_name = working_dir / f'{args.timings_file}'
    timings_file_obj  = open(timings_file_name, 'w')
    
    # set up the main logger file and list all relevant parameters.
    main_logger.info(f'Starting dask pipeline and setting up IO files. Time: {time.time()}\nRUN PARAMETERS:\nWorking directory: {args.working_dir}\nScheduler file: {args.scheduler_file}\nTiming file: {str(timings_file_name)}\nPRMTOP file: {PRMTOP_FILE}\nINPCRD file: {INPCRD_FILE}\nOutput path: {str(full_working_dir)}')
    # gather dask parameters too
    dask_parameter_string = ''
    for key, value in dask.config.config.items():
        dask_parameter_string += f"'{key}': '{value}'\n"
    dask_parameter_string += '################################################################################'
    main_logger.info(f'################################################################################\nDask parameters:\n{dask_parameter_string}')
    # report on the client and workers
    main_logger.info(f'Client information: {client}')
    NUM_WORKERS = get_num_workers(client)
    main_logger.info(f'Client is allocated {NUM_WORKERS} dask workers. So far.\n################################################################################')
    main_logger.info(f'Preparing to run {args.N_simulations} simulations.')

    #write header to timing csv file.
    timings_csv = csv.DictWriter(timings_file_obj,['hostname','worker_id','start_time','stop_time','run_number','n_steps'])
    timings_csv.writeheader()

    ## create list of run strings
    #temp_string = 'run_%0' + str(len(str(args.N_simulations))-1) + 'd/'
    #run_strings = [temp_string %(i) for i in range(args.N_simulations)]
    
    # using uuids instead of run_strings so that we can continually write 
    #trajectories to the same run directory without worry of overwriting 
    #files or hitting exceptions
    run_strings = [str(uuid4()) for i in range(args.N_simulations)]
    
    # do the thing.
    main_logger.info(f'Submitting tasks at {time.time()}.')
    task_futures = client.map(submit_pipeline,run_strings, prmtop_file = PRMTOP_FILE, inpcrd_file = INPCRD_FILE, outer_directory = full_working_dir, pure=False) 

    # gather results.
    ac = as_completed(task_futures)
    count = 0
    for finished_task in ac:
        hostname, worker_id, start_time, stop_time, run_number, n_steps = finished_task.result()
        # print summary to logging file
        if n_steps <= 0:
            main_logger.info(f'{run_number} failed to run. Check why. {args.N_simulations - count - 1} simulations left.')
        else:
            main_logger.info(f'{run_number} ran {n_steps} in {stop_time - start_time} seconds. {args.N_simulations - count - 1} simulations left.')
        # print timing info out
        append_timings(timings_csv,timings_file_obj,hostname,worker_id,start_time,stop_time,run_number,n_steps)
        count += 1

    main_logger.info(f'All tasks done. Cleaning up. Time: {time.time()}')
    # close logging and timing files
    clean_logger(main_logger)
    timings_file_obj.close()

