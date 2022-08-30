
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

timing_files = glob.glob("*/timings.csv")
timing_file_paths = [Path(f) for f in timing_files]

temp_dict = {'batch_start': 0,
             'batch_end': 0, 
             'client_start': 0,
             'client_end': 0, 
             'total_time': 0., 
             'client_start_overhead': [], 
             'worker_between_overhead': [], 
             'worker_idle_times': [],
             'stop_overhead': 0}

results_dict = {}

for timing_file in timing_file_paths:
    
    subdir = timing_file.parent.name
    
    results_dict[subdir] = copy.deepcopy(temp_dict)

    jobid = list(timing_file.parent.glob('*hosts'))[0].stem
    with open(f'out.{jobid}','r') as stdout:
        lines = stdout.readlines()
        start_time = lines[0].strip()
        end_time   = lines[-1].strip()

    st = datetime.strptime(start_time, "%a %b %d %H:%M:%S %Z %Y")
    et = datetime.strptime(end_time,   "%a %b %d %H:%M:%S %Z %Y")
    st_epoch = st.timestamp()

    with open(f'{subdir}/tskmgr.stdout','r') as stdout:
        lines = stdout.readlines()
        client_start = float(lines[0].split()[-1])
        client_end   = float(lines[-1].split()[-1])

    results_dict[subdir]['batch_start'] = 0
    results_dict[subdir]['batch_end'] = et.timestamp() - st_epoch
    results_dict[subdir]['client_start'] = client_start - st_epoch
    results_dict[subdir]['client_end'] = client_end - st_epoch

    df = pandas.read_csv(f'{subdir}/timings.csv', sep=',')
    df['start_time'] -= st_epoch
    df['stop_time']  -= st_epoch
    df = df.sort_values(by = ['start_time'])

    worker_dict = {}
    cpu_workers = []
    count = 0 
    for worker in df[df['task'] == 'preprocessing'].iloc:
        if worker['workerID'] not in worker_dict.keys():
            worker_dict[worker['workerID']] = count
            cpu_workers.append(worker['workerID'])
            count += 1

    gpu_workers = []
    for worker in df[df['task'] == 'processing'].iloc:
        if worker['workerID'] not in worker_dict.keys():
            worker_dict[worker['workerID']] = count
            gpu_workers.append(worker['workerID'])
            count += 1

    cpu_worker_client_startup_overheads = []
    cpu_worker_between_overheads = []
    cpu_worker_idle_times = []
    for cpu_worker in cpu_workers:
        cpu_worker_tasks = df[df['workerID'] == cpu_worker]
        a = np.min(cpu_worker_tasks['start_time']) - results_dict[subdir]['client_start']
        b = results_dict[subdir]['client_end'] - np.max(cpu_worker_tasks['stop_time'])
        cpu_worker_client_startup_overheads.append(a)
        cpu_worker_idle_times.append(b)
        between_time = 0
        for i in range(len(cpu_worker_tasks)-1):
            between_time += cpu_worker_tasks.iloc[i+1]['start_time'] - cpu_worker_tasks.iloc[i]['stop_time']
        cpu_worker_between_overheads.append(between_time)

    results_dict[subdir]['client_start_overhead'].append(cpu_worker_client_startup_overheads)
    results_dict[subdir]['worker_idle_times'].append(cpu_worker_idle_times)
    results_dict[subdir]['worker_between_overhead'].append(cpu_worker_between_overheads)

    gpu_worker_client_startup_overheads = []
    gpu_worker_between_overheads = []
    gpu_worker_idle_times = []
    for gpu_worker in gpu_workers:
        gpu_worker_tasks = df[df['workerID'] == gpu_worker]
        a = np.min(gpu_worker_tasks['start_time']) - results_dict[subdir]['client_start']
        b = results_dict[subdir]['client_end'] - np.max(gpu_worker_tasks['stop_time'])
        gpu_worker_client_startup_overheads.append(a)
        gpu_worker_idle_times.append(b)
        between_time = 0
        for i in range(len(gpu_worker_tasks)-1):
            between_time += gpu_worker_tasks.iloc[i+1]['start_time'] - gpu_worker_tasks.iloc[i]['stop_time']
        gpu_worker_between_overheads.append(between_time)

    results_dict[subdir]['client_start_overhead'].append(gpu_worker_client_startup_overheads)
    results_dict[subdir]['worker_idle_times'].append(gpu_worker_idle_times)
    results_dict[subdir]['worker_between_overhead'].append(gpu_worker_between_overheads)

    total_time = np.max([results_dict[subdir]['batch_end'],results_dict[subdir]['client_end'],np.max(df['stop_time'])])
    results_dict[subdir]['total_time'] = total_time
    results_dict[subdir]['stop_overhead'] = total_time - np.max(df['stop_time'])

    # plotting worker timeline
    colormap = cm.Dark2.colors
    figure = plt.figure(figsize=(8,10))
    ax = plt.gca()
    #fig, ax = plt.subplots(1)
    ax.plot([0,0],[0,count+1],'k--',zorder=3)
    ax.plot([results_dict[subdir]['client_start'],results_dict[subdir]['client_start']],[0,count+1],'r-',zorder=3)
    ax.plot([total_time,total_time],[0,count+1],'r-',zorder=3)
    
    for task in df.iloc:
        worker_id = worker_dict[task['workerID']]
        if task['task'] == 'preprocessing':
            facecolor = 'xkcd:green'
        elif task['task'] == 'processing':
            facecolor = 'xkcd:purple'
        elif task['task'] == 'postprocessing':
            facecolor = 'xkcd:orange'
        task_rect = Rectangle((task['start_time'], worker_id + 0.6), # xy
                               task['stop_time']-task['start_time'], # width
                               0.8, # height
                               facecolor = facecolor,
                               alpha = 0.75, edgecolor = 'xkcd:black', zorder = 3)
        ax.add_patch(task_rect)
    
    ax.set_ylim((0.,count+1))
    #ax.set_xlim((-1.,100))
    ax.set_ylabel('Worker Index',size=14)
    ax.set_xlabel('Time (sec)',size=14)
    plt.grid(b=True,which='major',axis='both',color='#808080',linestyle='--',alpha=0.75,zorder=1)
    plt.savefig(f'{subdir}/{subdir}_worker_timeline.png',dpi=600,transparent=True)
    plt.close()

with open('results_dictionary.pkl','wb') as out_pickle:
    pickle.dump(results_dict,out_pickle)

