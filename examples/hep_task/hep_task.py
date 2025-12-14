

import os, json
from maestro_lightning import Flow, Task, Dataset, Image


basepath         = os.getcwd()
input_path       = f"{basepath}/jobs"
number_of_events = 10
number_of_jobs   = 2
run_number       = 20251211
image_path       = '/mnt/shared/storage03/projects/cern/data/images/lorenzetti_latest.sif'
repo_build_path  = '/home/joao.pinto/git_repos/lorenzetti/build'
binds            = {"/mnt/shared/storage03" : "/mnt/shared/storage03"}


os.makedirs(input_path, exist_ok=True)
for job_id in range(number_of_jobs):
    with open(f"{input_path}/job_{job_id}.json", 'w') as f:
        nov = int(number_of_events / number_of_jobs)
        d = {
            'run_number'        : run_number,
            'seed'              : int(128 * (1+job_id)),
            'number_of_events'  : nov,
            #'events_per_job'    : int(nov/4),
            #'number_of_threads' : 4
        }
        json.dump(d,f)



with Flow(name="local_provider", path=f"{basepath}/local_tasks") as session:


    input_dataset    = Dataset(name="jobs", path=input_path)
    image            = Image(name="lorenzetti", path=image_path)


    pre_exec = f"source {repo_build_path}/lzt_setup.sh"

    command = f"{pre_exec} && gen_zee.py -o %OUT --job-file %IN -m"

    task_1 = Task(name="task_1.EVT",
                  image=image,
                  command=command,
                  input_data=input_dataset,
                  outputs={'OUT':'Zee.EVT.root'},
                  partition='cpu-large',
                  binds=binds)
    
    
    command = f"{pre_exec} && simu_trf.py -i %IN -o %OUT -nt 40"
    task_2 = Task(name="task_2.HIT",
                  image=image,
                  command=command,
                  input_data=task_1.output('OUT'),
                  outputs= {'OUT':'Zee.HIT.root'},
                  partition='cpu-large',
                  binds=binds)
    
    
    command = f"{pre_exec} && digit_trf.py -i %IN -o %OUT -nt 40"
    task_3 = Task(name="task_3.ESD",
                  image=image,
                  command=command,
                  input_data=task_2.output('OUT'),
                  outputs= {'OUT':'Zee.ESD.root'},
                  partition='cpu-large',
                  binds=binds)
    
    command = f"{pre_exec} && reco_trf.py -i %IN -o %OUT -nt 40"
    task_4 = Task(name="task_4.AOD",
                  image=image,
                  command=command,
                  input_data=task_3.output('OUT'),
                  outputs= {'OUT':'Zee.AOD.root'},
                  partition='cpu-large',
                  binds=binds)
    
    command = f"{pre_exec} && ntuple_trf.py -i %IN -o %OUT -nt 40"
    task_5 = Task(name="task_5.NTUPLE",
                  image=image,
                  command=command,
                  input_data=task_4.output('OUT'),
                  outputs= {'OUT':'Zee.NTUPLE.root'},
                  partition='cpu-large',
                  binds=binds)
   
   
    session.run()
    
