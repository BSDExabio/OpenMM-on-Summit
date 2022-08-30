
import sys
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib import cm
import numpy as np
import pandas
from datetime import datetime
import copy
import glob
from pathlib import Path
import pickle

subdirs = ['001_node/',     # 18 simulations
           '002_node/',     # 36 simulations
           'double_dask/',  # 180 simulations, two dask processes
           '045_node/',     # 810 simulations
           '500_node/',     # 9000 simulations
           '921_node/',     # 16578 simulations 
           '1000_node/',    # 18000 simulations
           '1500_node_18000_tasks/',    # 18000 simulations
           'double_dask_1000_nodes/',   # 18000 simulations
           'quad_dask_2000_nodes/']     # 36000 simulations

#timing_files = glob.glob("*/timings.csv")
timing_files = []
for subdir in subdirs:
    timing_files += list(glob.glob(f'{subdir}timings*csv'))
timing_files.sort()

print(timing_files)

timing_file_paths = [Path(f) for f in timing_files]

temp_dict = {'batch_start': 0,
             'batch_end': 0, 
             'client_start': 0,
             'client_end': 0, 
             'total_time': 0., 
             'worker_task_times': [], 
             'worker_start_overhead': [], 
             'worker_idle_times': [],
             'worker_between_overhead': [], 
             'stop_overhead': 0}

results_dict = {}

for timing_file in timing_file_paths:
    
    subdir = timing_file.parent.name
    
    results_dict[str(timing_file)] = copy.deepcopy(temp_dict)

    stdout_file = list(timing_file.parent.glob('md.*.out'))[0]
    
    # check for multiple timings.csv and tskmgr.log files in a subdir; indicative of multiple dask processes running. 
    try: 
        version_number = int(timing_file.stem[-1])
    except:
        version_number = 0

    if version_number: 
        tskmgr_file = list(timing_file.parent.glob(f'tskmgr{version_number}.log'))[0]
    else:
        tskmgr_file = list(timing_file.parent.glob('tskmgr.log'))[0]
   
    with open(stdout_file,'r') as stdout:
        lines = stdout.readlines()
        start_time   = lines[0].strip()
        end_time     = lines[-1].strip()

    with open(tskmgr_file,'r') as tskmgr:
        lines = tskmgr.readlines()
        client_start = float(lines[0].split()[-1])
        client_end   = float(lines[-1].split()[-1])

    st = datetime.strptime(start_time, "%a %b %d %H:%M:%S %Z %Y")
    et = datetime.strptime(end_time,   "%a %b %d %H:%M:%S %Z %Y")
    st_epoch = st.timestamp()

    results_dict[str(timing_file)]['batch_start'] = 0
    results_dict[str(timing_file)]['batch_end'] = et.timestamp() - st_epoch
    results_dict[str(timing_file)]['client_start'] = client_start - st_epoch
    results_dict[str(timing_file)]['client_end'] = client_end - st_epoch

    df = pandas.read_csv(timing_file, sep=',')
    df['start_time'] -= st_epoch
    df['stop_time']  -= st_epoch
    df = df.sort_values(by = ['start_time'])

    worker_dict = {}
    count = 0 
    for task in df.iloc:
        if task['worker_id'] not in worker_dict.keys():
            worker_dict[task['worker_id']] = count
            count += 1
        results_dict[str(timing_file)]['worker_task_times'].append(task['stop_time'] - task['start_time'])
        
    for worker in worker_dict.keys():
        worker_tasks = df[df['worker_id'] == worker]
        a = np.min(worker_tasks['start_time']) - results_dict[str(timing_file)]['client_start']
        b = results_dict[str(timing_file)]['client_end'] - np.max(worker_tasks['stop_time'])
        results_dict[str(timing_file)]['worker_start_overhead'].append(a)
        results_dict[str(timing_file)]['worker_idle_times'].append(b)
        between_time = 0
        for i in range(len(worker_tasks)-1):
            between_time += worker_tasks.iloc[i+1]['start_time'] - worker_tasks.iloc[i]['stop_time']
        results_dict[str(timing_file)]['worker_between_overhead'].append(between_time)

    total_time = np.max([results_dict[str(timing_file)]['batch_end'],results_dict[str(timing_file)]['client_end'],np.max(df['stop_time'])])
    results_dict[str(timing_file)]['total_time'] = total_time
    results_dict[str(timing_file)]['stop_overhead'] = total_time - np.max(df['stop_time'])

    if count < 200:
        # plotting worker timeline
        colormap = cm.Dark2.colors
        figure = plt.figure(figsize=(8,10))
        ax = plt.gca()
        ax.plot([0,0],[0,count+1],'k--',zorder=3)
        ax.plot([results_dict[str(timing_file)]['client_start'],results_dict[str(timing_file)]['client_start']],[0,count+1],'r-',zorder=3)
        ax.plot([total_time,total_time],[0,count+1],'r-',zorder=3)
        
        for task in df.iloc:
            worker_id = worker_dict[task['worker_id']]
            facecolor = 'xkcd:green'
            task_rect = Rectangle((task['start_time'], worker_id + 0.6), # xy
                                   task['stop_time']-task['start_time'], # width
                                   0.8, # height
                                   facecolor = facecolor,
                                   alpha = 0.75, edgecolor = 'xkcd:black', zorder = 3)
            ax.add_patch(task_rect)
        
        ax.set_ylim((0.5,count+0.5))
        #ax.set_xlim((-1.,100))
        ax.set_ylabel('Worker Index',size=14)
        ax.set_xlabel('Time (sec)',size=14)
        plt.grid(b=True,which='major',axis='both',color='#808080',linestyle='--',alpha=0.75,zorder=1)
        if version_number: 
            plt.savefig(f'{subdir}/{subdir}_{version_number}_worker_timeline.png',dpi=600,transparent=True)
        else:
            plt.savefig(f'{subdir}/{subdir}_worker_timeline.png',dpi=600,transparent=True)
        plt.close()

with open('results_dictionary.pkl','wb') as out_pickle:
    pickle.dump(results_dict,out_pickle)

