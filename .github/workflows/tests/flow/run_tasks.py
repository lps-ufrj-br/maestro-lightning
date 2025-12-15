import os, json
from maestro_lightning import Flow, Task, Dataset


basepath = os.getcwd()
input_path = f"{basepath}/jobs"
os.makedirs(input_path, exist_ok=True)
for i in range(2):
    with open(f"{input_path}/job_{i}.json",'w') as f:
        d={'input': i}
        json.dump(d,f)



with Flow(name="local_provider", path=f"{basepath}/local_tasks") as session:


    input_dataset_1  = Dataset(name="jobs", path=f"{basepath}/jobs")

    command = f"python3 {basepath}/run_job.py --job %IN --output %OUT"


    task_1 = Task(name="task_1",
                  command=command,
                  input_data=input_dataset_1,
                  outputs={'OUT':'output.json'},
                  partition='cpu-large',
                  )
    
    task_2 = Task(name="task_2",
                  command=command,
                  input_data=task_1.output('OUT'),
                  outputs= {'OUT':'output.json'},
                  partition='cpu-large',
                  )

    session.run(dry_run=True)
    
