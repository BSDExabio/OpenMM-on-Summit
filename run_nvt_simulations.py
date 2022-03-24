
import sys
import time
import os
import stat
import openmm
import parmed

prmtop = sys.argv[1]
rst7   = sys.argv[2]
run_num= sys.argv[3]
output_dir = sys.argv[4]
file_naming = output_dir + run_num + prmtop.split('/')[-1].split('.')[0] 

os.makedirs(f'{output_dir}{run_num}')
#os.chmod(f'{output_dir}{run_num}', stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)

time_step   = 2*openmm.unit.femtosecond	# simulation timestep
ts_float    = 2*10**-6			# femtoseconds to nanosecond
temperature = 310*openmm.unit.kelvin     # simulation temperature
friction    = 1/openmm.unit.picosecond   # collision rate
pressure    = 1.01325*openmm.unit.bar	# simulation pressure 
mcbarint    = 100			# number of steps between volume change attempts
num_steps   = 250000                     # number of integration steps to run
trj_freq    = 50000                      # number of steps per written trajectory frame
data_freq   = 50000                      # number of steps per written simulation statistics

with open(f'{output_dir}{run_num}simulation.log','w') as logging_file:
    # SETTING up the simulation system
    logging_file.write(f'Starting at {time.time()}\n')
    logging_file.write('Loading the AMBER files.\n')
    parmed_system = parmed.load_file(prmtop,rst7)
    logging_file.write('Creating PARMED system.\n')
    system = parmed_system.createSystem(nonbondedMethod=openmm.app.PME, nonbondedCutoff=1.2*openmm.unit.nanometer,constraints=openmm.app.HBonds)
    
    # SETTING up the Langevin dynamics thermostat.
    logging_file.write('Creating OpenMM integrator.\n')
    integrator = openmm.LangevinIntegrator(temperature, friction, time_step)
    
    # SETTING the simulation platform .
    logging_file.write('Choosing OpenMM platform.\n')
    platform = openmm.Platform.getPlatformByName('CUDA')
    #prop     = dict(CudaPrecision='mixed') # Use mixed single/double precision
    
    # SETTING up an OpenMM simulation.
    logging_file.write('Creating OpenMM simulation engine.\n')
    simulation = openmm.app.Simulation(parmed_system.topology, system, integrator, platform)
    
    # SETTING the initial positions.
    logging_file.write('Setting starting positions.\n')
    simulation.context.setPositions(parmed_system.positions)
    
    # SETTING the velocities from a Boltzmann distribution at a given temperature.
    logging_file.write('Setting starting velocities.\n')
    simulation.context.setVelocitiesToTemperature(temperature)
    
    # SETTING up output files.
    logging_file.write('Setting reporter files.\n')
    simulation.reporters.append(openmm.app.dcdreporter.DCDReporter(f'{file_naming}.nvt_equilib.dcd',trj_freq))
    simulation.reporters.append(openmm.app.statedatareporter.StateDataReporter(f'{file_naming}.nvt_equilib.out',data_freq,step=True,potentialEnergy=True,kineticEnergy=True,temperature=True,volume=True,density=True,speed=True))
    
    # RUNNING the simulation
    logging_file.write("Starting simulation.\n")
    start = time.time()
    simulation.step(num_steps)
    end = time.time()
    logging_file.write("Simulation elapsed time %.2f seconds\nAverage speed: %.3f ns day^{-1}.\n" % (end-start,(num_steps*ts_float*86400)/(end-start)))
    logging_file.write("Done!\n")
    logging_file.write(f'Ending at {time.time()}')
    
    simulation.saveCheckpoint(file_naming+'.nvt_equilib.chkpt')
    simulation.saveState(file_naming+'.nvt_equilib.stt')
    
os.chmod(f'{output_dir}{run_num}simulation.log', stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
os.chmod(f'{file_naming}.nvt_equilib.dcd', stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
os.chmod(f'{file_naming}.nvt_equilib.out', stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)

