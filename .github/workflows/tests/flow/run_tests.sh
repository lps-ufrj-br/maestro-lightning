
basepath=$PWD
python run_tasks.py 
maestro run task -t $basepath/local_tasks/flow.json -i 0 --dry-run
maestro run job -i $basepath/local_tasks/tasks/task_1/jobs/inputs/job_0.json -o $basepath/local_tasks/tasks/task_1/works/job_0
maestro run job -i $basepath/local_tasks/tasks/task_1/jobs/inputs/job_1.json -o $basepath/local_tasks/tasks/task_1/works/job_1
maestro run next -t $basepath/local_tasks/flow.json -i 0 --dry-run
maestro run task -t $basepath/local_tasks/flow.json -i 1 --dry-run
maestro run job -i $basepath/local_tasks/tasks/task_2/jobs/inputs/job_0.json -o $basepath/local_tasks/tasks/task_2/works/job_0
maestro run job -i $basepath/local_tasks/tasks/task_2/jobs/inputs/job_1.json -o $basepath/local_tasks/tasks/task_2/works/job_1
maestro run next -t $basepath/local_tasks/flow.json -i 1 --dry-run
