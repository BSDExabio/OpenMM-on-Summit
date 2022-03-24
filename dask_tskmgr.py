#!/usr/bin/env python3
""" Task manager for running the dask pipeline for OpenMM molecular dynamics simulations on Summit. 

    Ingests a set of prmtop and inpcrd files and distributes work to run MD simulations of them among allocated dask workers. The dask workers will be assigned to a single GPU and associated CPUs. 

    USAGE: 
        python3 dask_taskmgr.py [-h] [--scheduler-timeout SCHEDULER_TIMEOUT] --scheduler-file SCHEDULER_FILE --input-file INPUT_FILE --timings-file TIMINGS_FILE.csv --working-dir /path/to/dir/ --script-path /path/to/dir/script.py

    INPUT: 
        -h, --help      show this help message and exit
        --scheduler-timeout SCHEDULER_TIMEOUT, -t SCHEDULER_TIMEOUT 
                        dask scheduler timeout; default: 5000 seconds
        --scheduler-file SCHEDULER_FILE, -s SCHEDULER_FILE
                        dask scheduler file
        --N-simulations number_of_simulations, -n number_of_simulations
                        integer used to set the number of total simulations (tasks) that will be performed
        --timings-file TIMINGS_FILE.csv, -ts TIMINGS_FILE.csv 
                        CSV file for task processing timings
        --working-dir /path/to/dir/, -wd /path/to/dir/
                        full path to the directory within which files will be written
        --script-path /path/to/dir/script.py, -sp /path/to/dir/script.py
                        full path to the python script to be run within the subprocess

    HARD CODED VARIABLES:
        PRMTOP_FILE:
        INPCRD_FILE:
        DIRECTORY: name of the directory within which output for each task will be written
"""

import time
import argparse
import platform
import os
import stat
import traceback
import numpy as np

import sys

import csv

import subprocess
from subprocess import CalledProcessError

import dask.config
from distributed import Client, Worker, as_completed, get_worker

import logging_functions

# NOTE: hard coded variables for the moment.
PRMTOP_FILE = '/gpfs/alpine/proj-shared/bip198/dask_testing/md_simulations/enam_726.prmtop'
INPCRD_FILE = '/gpfs/alpine/proj-shared/bip198/dask_testing/md_simulations/enam_726.ncrst'
DIRECTORY   = 'enam_726'

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
                   end_string):
    """ append the task timings to the CSV timings file
    :param csv_writer: CSV to which to append timings
    :param hostname: on which the processing took place
    :param worker_id: of the dask worker that did the processing
    :param start_time: start time in *NIX epoch seconds
    :param stop_time: stop time in same units
    :param end_string: that was processed
    """
    csv_writer.writerow({'hostname'  : hostname,
                         'worker_id' : worker_id,
                         'start_time': start_time,
                         'stop_time' : stop_time,
                         'run_num'   : end_string})
    file_object.flush()


def submit_pipeline(run_number,script='',prmtop_file='',inpcrd_file='',save_directory='',working_directory=''):
    """
    """
    worker = get_worker()
    start_time = time.time()

    try:
        completed_process = subprocess.run(f'python3 {script} {prmtop_file} {inpcrd_file} {run_number} {save_directory}',shell=True,capture_output=True,check=True,cwd=working_directory)
        simulation_path = completed_process.stdout.decode('utf-8').strip()
        return platform.node(), worker.id, start_time, time.time(), simulation_path

    except CalledProcessError as e:
        print(e)
        return platform.node(), worker.id, start_time, time.time(), f'failed to complete {run_number}'

    except Exception as e:
        print(e)
        return platform.node(), worker.id, start_time, time.time(), f'failed to complete {run_number}'


#######################################
### MAIN
#######################################

if __name__ == '__main__':
    # read command line arguments.
    parser = argparse.ArgumentParser(description='Molecular dynamics simulation task manager')
    parser.add_argument('--scheduler-timeout', '-t', default=5000, type=int, help='dask scheduler timeout')
    parser.add_argument('--scheduler-file', '-s', required=True, help='dask scheduler file')
    parser.add_argument('--N-simulations', '-n', required=True, type=int, help='integer used to set the number of total simulations (tasks) that will be performed')
    parser.add_argument('--timings-file', '-ts', required=True, help='CSV file for protein processing timings')
    parser.add_argument('--working-dir', '-wd', required=True, help='path that points to the working directory for the output files')
    parser.add_argument('--script-path', '-sp', required=True, help='path that points to the script for the subprocess call')
    args = parser.parse_args()

    if args.working_dir[-1] != os.path.sep:
        args.working_dir += os.path.sep
    if DIRECTORY[-1] != os.path.sep:
        DIRECTORY += os.path.sep

    # set up the main logger file and list all relevant parameters.
    main_logger = logging_functions.setup_logger('tskmgr_logger',f'{args.working_dir}tskmgr.log')
    main_logger.info(f'Starting dask pipeline and setting up logging. Time: {time.time()}')
    main_logger.info(f'Scheduler file: {args.scheduler_file}')
    main_logger.info(f'Scheduler timeout: {args.scheduler_timeout}')
    main_logger.info(f'Timing file: {args.timings_file}')
    main_logger.info(f'Working directory: {args.working_dir}')
    main_logger.info(f'Path to subprocess script: {args.script_path}')
    main_logger.info(f'PRMTOP file: {PRMTOP_FILE}')
    main_logger.info(f'INPCRD file: {INPCRD_FILE}')
    main_logger.info(f'Output path: {args.working_dir}')
    dask_parameter_string = ''
    for key, value in dask.config.config.items():
        dask_parameter_string += f"'{key}': '{value}'\n"
    dask_parameter_string += '################################################################################'
    main_logger.info(f'Dask parameters:\n{dask_parameter_string}')

    # create list of run strings
    temp_string = 'run_%0' + str(len(str(args.N_simulations))-1) + 'd/'
    run_strings = [temp_string %(i) for i in range(args.N_simulations)]
    main_logger.info(f'Preparing to run {args.N_simulations} simulations.')

    # set up timing log file.
    timings_file = open(args.timings_file, 'w')
    timings_csv = csv.DictWriter(timings_file,['hostname','worker_id','start_time','stop_time','run_num'])
    timings_csv.writeheader()

    # start dask client.
    client = Client(scheduler_file=args.scheduler_file,timeout=args.scheduler_timeout,name='simulationtaskmgr')
    main_logger.info(f'Client information: {client}')
    NUM_WORKERS = get_num_workers(client)
    main_logger.info(f'Starting with {NUM_WORKERS} dask workers.')
    
    # wait for workers.
    wait_start = time.time()
    client.wait_for_workers(n_workers=NUM_WORKERS)
    main_logger.info(f'Waited for {NUM_WORKERS} workers took {time.time() - wait_start} sec')
    workers_info = client.scheduler_info()['workers']
    connected_workers = len(workers_info)
    main_logger.info(f'{connected_workers} workers connected.\n################################################################################')

    # do the thing.
    task_futures = client.map(submit_pipeline,run_strings, script = args.script_path, prmtop_file = PRMTOP_FILE, inpcrd_file = INPCRD_FILE, save_directory = DIRECTORY, working_directory = args.working_dir, pure=False) 

    # gather results.
    ac = as_completed(task_futures)
    for i, finished_task in enumerate(ac):
        results = finished_task.result()
        hostname, worker_id, start_time, stop_time, end_string = finished_task.result()
        if 'failed' in end_string:
            main_logger.info(f'{end_string}')
            main_logger.info(f'{args.N_simulations - i - 1} simulations left')
            append_timings(timings_csv,timings_file,hostname,worker_id,start_time,stop_time,end_string)
        else:
            main_logger.info(f'{end_string} processed in {(stop_time - start_time) / 60.} minutes.')
            main_logger.info(f'{args.N_simulations - i - 1} simulations left')
            append_timings(timings_csv,timings_file,hostname,worker_id,start_time,stop_time,end_string)

    # close log files and shut down the cluster.
    timings_file.close()
    os.chmod(args.timings_file, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
    main_logger.info(f'Done. Shutting down the cluster. Time: {time.time()}')
    logging_functions.clean_logger(main_logger)
    os.chmod('tskmgr.log', stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
    client.shutdown()

