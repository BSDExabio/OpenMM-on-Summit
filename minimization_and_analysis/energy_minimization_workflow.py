#!/usr/bin/env python3
""" Task manager for running a dask workflow for OpenMM energy minimization 
    pipeline. Three steps:
        System preparation. Add missing atoms (mainly hydrogens) and prep a pdb 
            file for loading into OpenMM. Run on CPU resource workers. 
        Energy minimization. Perform the OpenMM energy minimization calculation 
            on GPU resource workers.
        Post-analysis, a centering script to remove translational motion as an 
            example. Run on CPU resource workers. 

    USAGE: 
        python3 energy_minimization_workflow.py [-h] --scheduler-file SCHEDULER_FILE \
                                                     --list-file INPUT_FILE \
                                                     --output-dir /path/to/dir/ \
                                                     --parameter-file OPENMM_DICT.pkl \
                                                     --tskmgr-log-file log_file.log \
                                                     --timings-file TIMINGS_FILE.csv 

    INPUT: 
        -h, --help      show this help message and exit
        --scheduler-file SCHEDULER_FILE, -s SCHEDULER_FILE
                        dask scheduler file
        --list-file INPUT_FILE, -inp INPUT_FILE
                        file with paths and sequence lengths of protein structures to be submitted to the workflow
        --output-dir /path/to/dir/, -wd /path/to/dir/
                        full path to the directory within which files will be written
        --parameter-file OPENMM_DICT.pkl, -param OPENMM_DICT.pkl
                        pickle file containing a dictionary with parameters used in OpenMM simulations
        --tskmgr-log-file TSKMGR.log, -log TSKMGR.log
                        path to a new log file within which logging information will be pritned
        --timings-file TIMINGS_FILE.csv, -ts TIMINGS_FILE.csv 
                        CSV file for task processing timings
"""


import numpy as np
import pdbfixer
import openmm

import logging
import argparse
import sys
import os
import time
import csv
import stat
import traceback
from pathlib import Path
import pickle

import platform
import dask.config
from distributed import Client, Worker, as_completed, get_worker

#######################################
### PRE-PROCESSING FUNCTION
#######################################

def fix_protein(pdb_file, root_output_path = Path('./')):
    """ Check the structure model for any fixes before sending it to be parameterized and minimized.
    INPUT:
        pdb_file: pathlib Path object pointing twoards the pdb structure file that is to be "fixed". 
        output_path: pathlib Path object pointing to where the "fixed" pdb file will be written. 
    
    RETURNS:
        output_dict: dictionary containing all relevant info and objects for the run. Includes:
            'task': string that describes this function's purpose
            'nodeID', 'workerID': information about hardware being used for this calculation
            'logger_file': pathlib Path object pointing to the system's specific log file
            'output_pdb_file': path object pointing to the final energy minimized structure 
            'start_time', 'stop_time': floats, time values
            'return_code': integer value denoting the success of this function. 
    """
    # prepping the output dictionary object with initial values
    output_dict = {'task': 'preprocessing', 
                   'nodeID': platform.node(), 
                   'workerID': get_worker().id,
                   'start_time': time.time(),
                   'output_pdb_file': None,
                   'logger_file': None,
                   'stop_time' : None,
                   'return_code': None}

    # making directory for output
    output_path = root_output_path / pdb_file.parent.name     # NOTE: based on an assumed directory organization
    try:
        os.makedirs(output_path,mode=0o755)
    except FileExistsError:
        pass
    
    file_root = str(output_path / pdb_file.stem)
    # stashing
    output_dict['output_pdb_file'] = Path(file_root + '_fixed.pdb')
    output_dict['logger_file'] = Path(file_root + '.log')
    
    # setting up the individual run's logging file
    prep_logger = setup_logger(f'{pdb_file.stem}_minimization_logger', output_dict['logger_file'])
    prep_logger.info(f"Pre-processing being performed on {output_dict['nodeID']} with {output_dict['workerID']}.")
    prep_logger.info(f'     Checking {pdb_file!s} for any required fixes (missing hydrogens and other atoms, etc).')
    
    try:
        with open(pdb_file,'r') as pdb, open(output_dict['output_pdb_file'],'w') as save_file:
            fixer = pdbfixer.PDBFixer(pdbfile=pdb)
            fixer.findMissingResidues()
            fixer.findMissingAtoms()
            fixer.addMissingAtoms(seed=0)
            fixer.addMissingHydrogens()
            prep_logger.info(f"        Saving {output_dict['output_pdb_file']!s}.")
            openmm.app.pdbfile.PDBFile.writeFile(fixer.topology,fixer.positions,file=save_file)
        return_code = 0
    
    except Exception as e:
        prep_logger.info(f'For {pdb_file!s}: preprocessing code, fix_protein, failed with the following exception:\n{e}\n')
        return_code = 1

    finally:
        # stashing
        output_dict['return_code'] = return_code
        output_dict['stop_time'] = time.time()
        if output_dict['return_code'] == 0:
            prep_logger.info(f"Finished preprocessing {pdb_file!s}. This took {output_dict['stop_time']-output_dict['start_time']} seconds.\n")
        
        clean_logger(prep_logger)
        return output_dict


#######################################
### PROCESSING FUNCTIONS
#######################################

def will_restrain(atom: openmm.app.topology.Atom, rset: str) -> bool:
  """Returns True if the atom will be restrained by the given restraint set."""

  if rset == "non_hydrogen":
    return atom.element.name != "hydrogen"
  elif rset == "c_alpha":
    return atom.name == "CA"


def _add_restraints(
    system,             #system: openmm.System,
    reference_pdb,      #reference_pdb: openmm.PDBFile,
    stiffness,          #stiffness: openmm.unit.Unit,
    rset,               #rset: str,
    exclude_residues):  #exclude_residues: Sequence[int]):
  """Adds a harmonic potential that restrains the end-to-end distance."""
  
  assert rset in ["non_hydrogen", "c_alpha"]

  force = openmm.CustomExternalForce("0.5 * k * ((x-x0)^2 + (y-y0)^2 + (z-z0)^2)")
  force.addGlobalParameter("k", stiffness)
  for p in ["x0", "y0", "z0"]:
    force.addPerParticleParameter(p)

  for i, atom in enumerate(reference_pdb.topology.atoms()):
    if atom.residue.index in exclude_residues:
      continue
    if will_restrain(atom, rset):
      force.addParticle(i, reference_pdb.positions[i])
  system.addForce(force)


def run_minimization(input_dict, openmm_dictionary = {}):
    """run preparation of OpenMM simulation object and subsequent energy minimization
    INPUT:
        input_dict: dictionary containing all relevant info and objects needed to start the run for the specific system. At least includes values associated with these keys:
            'pdb_file': pathlib Path object associated with the "fixed" pdb structure file that is ready for simulation.
            'logger_file': pathlib Path object that points to a log file within which logging information is being written for this specific protein.
            'return_code': integer value denoting the success of the previous function; 0 == successful preprocessing.
        
        openmm_dictionary: dictionary object that contains all relevant parameters used for preparation and simulations in OpenMM; keys: 
            "forcefield": string, denotes the force field xml file to be used to prep parameters for the structure. Acceptable values: "amber99sb.xml", "amber14/protein.ff14SB.xml", "amber14/protein.ff15ipq.xml", "charmm36.xml", or others. NOTE: currently only accepts a single string so won't be able to include an implicit solvent model as an additional force field file. 
            "exclude_residues": list of residue indices to be ignored when setting restraints.
            "restraint_set": string used to denote which atoms within the structure are restrained.
            "restraint_stiffness": float, sets the restraint spring constant (units: energy_units length_units **-2) to be applied to all atoms defined in the restraint_set variable. 
            "max_iterations": int, the maximum number of minimization iterations that will be performed; default = 0, no limit to number of minimization calculations
            "energy_tolerance": float, the energy tolerance cutoff, below which, a structure is considered acceptably energy-minimized
            "fail_attempts": int, number of minimization attempts to try before giving up
            "energy_units": openmm unit object for energy
            "length_units": openmm unit object for length
    RETURNS:
        output_dict: dictionary containing all relevant info and objects for the run. Includes:
            'task': string that describes this function's purpose
            'nodeID', 'workerID': information about hardware being used for this calculation
            'logger_file': pathlib Path object pointing to the system's specific log file
            'output_pdb_file': path object pointing to the final energy minimized structure 
            'start_time', 'stop_time': floats, time values
            'return_code': integer value denoting the success of this function. 
    """
    # if the previous process failed, just close this task out. 
    if input_dict['return_code'] != 0:
        return {'return_code':1}
    # grab the important input_dict values
    logger_file = input_dict['logger_file']
    pdb_file    = input_dict['output_pdb_file']

    # prepping the output dictionary object with initial values
    output_dict = {'task': 'processing', 
                   'nodeID': platform.node(), 
                   'workerID': get_worker().id,
                   'start_time': time.time(),
                   'logger_file': logger_file,
                   'output_pdb_file': None,
                   'stop_time' : None,
                   'return_code': None}

    out_file_path = pdb_file.parent / pdb_file.stem
    # setting up the individual run's logging file
    proc_logger = setup_logger(f'{pdb_file.stem}_minimization_logger', logger_file)
    proc_logger.info(f"Prepping OpenMM simulation object and running energy minimization on {output_dict['nodeID']} with {output_dict['workerID']}.")
   
    # gathering openmm parameters
    forcefield          = openmm_dictionary['forcefield']
    exclude_residues    = openmm_dictionary['exclude_residues']
    restraint_set       = openmm_dictionary['restraint_set']
    restraint_stiffness = openmm_dictionary['restraint_stiffness']
    openmm_platform     = openmm_dictionary['openmm_platform']
    max_iterations      = openmm_dictionary['max_iterations']
    energy_tolerance    = openmm_dictionary['energy_tolerance']
    fail_attempts       = openmm_dictionary['fail_attempts']
    energy_units        = openmm_dictionary['energy_units']
    length_units        = openmm_dictionary['length_units']
    
    try: 
        start_time = time.time()
        proc_logger.info(f'Preparing the OpenMM simulation components:')
        # load pdb file into an openmm Topology and coordinates object.
        pdb = openmm.app.pdbfile.PDBFile(str(pdb_file))

        # set the FF and constraints objects.
        proc_logger.info(f'        Using {forcefield}.')
        force_field = openmm.app.forcefield.ForceField(forcefield)
        
        # prepare the restraints/constraints for the system.
        proc_logger.info(f'        Building HBond constraints as well as restraints on "{restraint_set}".')
        proc_logger.info(f'        Restraints have a spring constant of {restraint_stiffness} {energy_units} {length_units}^-2.')
        proc_logger.info(f'        ResIDs {exclude_residues} are not included in the restraint set.')
        constraints = openmm.app.HBonds
        system = force_field.createSystem(pdb.topology, constraints=constraints)
        stiffness = restraint_stiffness * energy_units / (length_units**2)
        if stiffness > 0. * energy_units / (length_units**2):
            _add_restraints(system, pdb, stiffness, restraint_set, exclude_residues)
        
        # create the integrator object. 
        integrator = openmm.LangevinIntegrator(0, 0.01, 0.0)    # required to set this for prepping the simulation object; hard set because we won't be using it; still necessary to define for the creation of the simulation object
        
        # determine what hardware will be used to perform the calculations
        openmm_platform = openmm.Platform.getPlatformByName(openmm_platform)  
        
        # prep the simulation object
        simulation = openmm.app.Simulation(pdb.topology, system, integrator, openmm_platform)
        # set the atom positions for the simulation's system's topology
        simulation.context.setPositions(pdb.positions)
        # set the minimization energy convergence tolerance value
        tolerance = energy_tolerance * energy_units

        proc_logger.info(f'Finished prepping simulation components. This took {time.time()-start_time} seconds.')
        
        proc_logger.info(f'Running energy minimization.')
        start_time = time.time()
        
        # grab initial energies
        state = simulation.context.getState(getEnergy=True)
        einit = state.getPotentialEnergy().value_in_unit(energy_units)
        
        proc_logger.info(f'        Starting energy: {einit} {energy_units}')
        
        # attempt to minimize the structure
        attempts = 0
        minimized = False
        while not minimized and attempts < fail_attempts:
            attempts += 1
            try:
                # running minimization
                simulation.minimizeEnergy(maxIterations=max_iterations,tolerance=tolerance)
                
                # return energies and positions
                state = simulation.context.getState(getEnergy=True, getPositions=True)
                efinal = state.getPotentialEnergy().value_in_unit(energy_units)
                positions = state.getPositions(asNumpy=True).value_in_unit(length_units)
                proc_logger.info(f'        Final energy: {efinal} {energy_units}')
                 
                # saving the final structure to a pdb
                out_file = Path(str(out_file_path) + '_min_%02d.pdb'%(attempts-1))
                with open(out_file,'w') as out_pdb:
                    openmm.app.pdbfile.PDBFile.writeFile(simulation.topology,positions,file=out_pdb)
                minimized = True
                return_code = 0
            except Exception as e:
                proc_logger.info(f'        Attempt {attempts}: {e}')

        proc_logger.info(f'        dE = {efinal - einit} {energy_units}')
        output_dict['output_pdb_file'] = out_file
        
        if not minimized:
            proc_logger.info(f"Minimization failed after {fail_attempts} attempts.\n")
            return_code = 1
    
        proc_logger.info(f'Finished running minimization. This took {time.time()-start_time} seconds.')

    except Exception as e:
        proc_logger.info(f'Processing code, run_minimization, failed with the following exception:\n{e}\n')
        return_code = 1

    finally:
        output_dict['stop_time'] = time.time()
        output_dict['return_code'] = return_code
        
        if output_dict['return_code'] == 0:
            proc_logger.info(f"Finished the processing task. This took {output_dict['stop_time']-output_dict['start_time']} seconds.\n")
        clean_logger(proc_logger)
        
        return output_dict


#######################################
### POST-PROCESSING FUNCTIONS
#######################################

def center(input_dict):
    """function to take a pdb structure file and translate the structure's center of geometry to the origin. JUST AN EXAMPLE OF A POST-ANALYSIS FUNCTION 
    INPUT:
        input_dict: dictionary containing all relevant keys and values needed to start the run for the specific system. At least includes values associated with these keys:
            'pdb_file': pathlib Path object associated with the "fixed" pdb structure file that is ready for simulation.
            'logger_file': pathlib Path object that points to a log file within which logging information is being written for this specific protein.
            'return_code': integer value denoting the success of the previous function; 0 == successful preprocessing.
    
    RETURNS:
        output_dict: dictionary containing all relevant info and objects for the run. Includes:
            'task': string that describes this function's purpose
            'nodeID', 'workerID': information about hardware being used for this calculation
            'logger_file': pathlib Path object pointing to the system's specific log file
            'output_pdb_file': path object pointing to the final energy minimized structure 
            'start_time', 'stop_time': floats, time values
            'return_code': integer value denoting the success of this function. 
    """
    # if the previous process failed, just close this task out. 
    if input_dict['return_code'] != 0:
        return {'return_code':1}
    # grab the important input_dict values
    logger_file = input_dict['logger_file']
    pdb_file    = input_dict['output_pdb_file']
    
    # prepping the output dictionary object with initial values
    output_dict = {'task': 'postprocessing', 
                   'nodeID': platform.node(), 
                   'workerID': get_worker().id,
                   'start_time': time.time(),
                   'logger_file': logger_file,
                   'output_pdb_file': None,
                   'stop_time' : None,
                   'return_code': None}

    # setting up the individual run's logging file
    post_logger = setup_logger(f'{pdb_file.stem}_minimization_logger', logger_file)
    post_logger.info(f"Post-processing the energy minimized structure on {output_dict['nodeID']} with {output_dict['workerID']}.")
    post_logger.info(f"Removing CoG translation from the {pdb_file!s}.")
    
    out_file_path = logger_file.parent / logger_file.stem
    output_dict['output_pdb_file'] = Path(str(out_file_path) + '_centered.pdb')
    try:
        with open(pdb_file,'r') as pdb, open(output_dict['output_pdb_file'],'w') as out_pdb:
            line_lst = pdb.readlines()
            xyz_array = np.array([[float(line[30:38]),float(line[38:46]),float(line[46:54])] for line in line_lst if line.split()[0] == 'ATOM'])
            avg_xyz = np.mean(xyz_array,axis=0)
            xyz_array -= avg_xyz
            post_logger.info(f'         Translated the system by {-avg_xyz}.')
            post_logger.info(f'         CoG now at {np.mean(xyz_array,axis=0)}.')
            count = 0 
            for line in line_lst:
                if line.split()[0] == 'ATOM':
                    out_pdb.write(line[:30] + '%8.3f%8.3f%8.3f'%(xyz_array[count,0],xyz_array[count,1],xyz_array[count,2]) + line[54:])
                    count += 1
                else:
                    out_pdb.write(line)
        return_code = 0

    except Exception as e:
        print(str(out_file_path),str(e), file=sys.stderr, flush=True)
        post_logger.info(f'For {pdb_file!s}: postprocessing code, center, failed with the following exception:\n{e}\n')
        return_code = 1

    finally:
        output_dict['stop_time'] = time.time()
        output_dict['return_code'] = return_code
        if output_dict['return_code'] == 0:
            post_logger.info(f"Finished processing files {pdb_file!s} after {output_dict['stop_time']-output_dict['start_time']} seconds.\n")
        clean_logger(post_logger)
        
        return output_dict


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


def disconnect(client, workers_list):
    """ Shutdown the active workers in workers_list
    :param client: active dask client
    :param workers_list: list of dask workers
    """
    client.retire_workers(workers_list, close_workers=True)
    client.shutdown()


#######################################
### LOGGING FUNCTIONS
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


def append_timings(csv_writer, file_object, nodeID, workerID, start_time, stop_time, task, return_code, file_path):
    """ append the task timings to the CSV timings file
    :param csv_writer: CSV to which to append timings
    :param hostname: on which the processing took place
    :param worker_id: of the dask worker that did the processing
    :param start_time: start time in *NIX epoch seconds
    :param stop_time: stop time in same units
    :param end_string: that was processed
    """
    csv_writer.writerow({'nodeID'     : nodeID,
                         'workerID'   : workerID,
                         'start_time' : start_time,
                         'stop_time'  : stop_time,
                         'task'       : task,
                         'return_code': return_code,
                         'file_path'  : file_path})
    file_object.flush()


#######################################
### MAIN
#######################################

if __name__ == '__main__':
    print('Client script starting:',time.time())
   
    # read command line arguments.
    parser = argparse.ArgumentParser(description='Energy minimization task manager')
    parser.add_argument('--scheduler-file', '-s', required=True, help='dask scheduler file')
    parser.add_argument('--list-file', '-inp', required=True, help='file with paths and sequence lengths of protein structures to be submitted to the workflow')
    parser.add_argument('--output-dir', '-wd', required=True, help='path that points to the working directory for the output files')
    parser.add_argument('--parameters-file', '-param', required=True, help='pickle file containing a dictionary with parameters used in OpenMM simulations')
    parser.add_argument('--tskmgr-log-name', '-log', required=True, help='string that will be used to store logging info for this run')
    parser.add_argument('--timings-file', '-ts', required=True, help='CSV file for protein processing timings')
    
    args = parser.parse_args()

    # logger file
    main_logger = setup_logger('tskmgr_logger',output_dir + f'/{args.tskmgr_log_name}')
    main_logger.info(f'Starting dask pipeline. Time: {time.time()}\nRUN PARAMETERS:\nOutput directory: {args.output_dir}\nScheduler file: {args.scheduler_file}\nTiming file: {args.timings_file}\nStructure list file: {args.list_file}\nOpenMM parameters file: {args.parameters_file}')
    # gather dask parameters too
    dask_parameter_string = ''
    for key, value in dask.config.config.items():
        dask_parameter_string += f"'{key}': '{value}'\n"
    dask_parameter_string += '################################################################################'
    main_logger.info(f'################################################################################\nDask parameters:\n{dask_parameter_string}')
    # report on the client and workers
    main_logger.info(f'Client information: {client}')
    
    # spin up the client
    client = Client(scheduler_file=args.scheduler_file,timeout=5000,name='all_tsks_client')

    # assumes args.list_file is a string pointing to a list file that has "path_to_structure nResidues" on every line
    with open(args.list_file,'r') as structures_file:
        structure_list = [line.split() for line in structures_file.readlines() if line[0] != '#']
    
    # sorted largest to smallest of the structure list; system size is a basic but good estimate of computational cost
    sorted_structure_list = sorted(structure_list, key = lambda x: int(x[1]))[::-1]
    sorted_structure_paths= [Path(structure[0]) for structure in sorted_structure_list]
    main_logger.info(f'{len(structure_list)} structures to be processed.')

    root_output_path = Path(args.output_dir)
    
    with open(args.parameters_file,'rb') as pickle_file:
        openmm_dictionary = pickle.load(pickle_file)
    ### TO DO: check if all parameters have been explicitly defined; if they haven't, fill in with default values. # NOTE apply this code.

    # set up timing log file
    timings_file = open(args.timings_file,'w')
    timings_csv = csv.DictWriter(timings_file,['nodeID','workerID','start_time','stop_time','task','return_code','file_path'])
    timings_csv.writeheader()

    # do the thing.
    main_logger.info(f'Submitting tasks at {time.time()}')
    # submit preprocessing tasks
    preprocessing_futures = client.map(fix_protein, sorted_structure_paths, root_output_path = root_output_path, pure = False, resources={'CPU':1})
    # submit processing tasks, input preprocessing_futures
    processing_futures    = client.map(run_minimization, preprocessing_futures, openmm_dictionary = openmm_dictionary, pure = False, resources={'GPU':1})
    # submit postprocessing tasks, input processing_futures
    postprocessing_futures= client.map(center, processing_futures, pure = False, resources={'CPU':1})
    # gathering all futures into one list
    futures_bucket = preprocessing_futures + processing_futures + postprocessing_futures
    # create the as_completed iterator object for all tasks
    finished_bucket = as_completed(futures_bucket)
    for task in finished_bucket: 
        results_dictionary = task.result()
        # whether pre- processing, or post-processing, each task type will return similarly formatted output (in the form of a dictionary in this case) so we can process them similarly.
        append_timings(timings_csv,timings_file,results_dictionary['nodeID'],results_dictionary['workerID'],results_dictionary['start_time'],results_dictionary['stop_time'],results_dictionary['task'],results_dictionary['return_code'],str(results_dictionary['output_pdb_file'].parent))
        main_logger.info(f"{str(results_dictionary['output_pdb_file'].parent} finished with {results_dictionary['task']}. Return status = {results_dictionary['return_code']}.")

    print('Client script closing:',time.time())

