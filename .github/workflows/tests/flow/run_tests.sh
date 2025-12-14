python create_tasks.py 
maestro run task -t /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/flow.json -i 0 --dry-run
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/jobs/inputs/job_0.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/works/job_0
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/jobs/inputs/job_1.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/works/job_1
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/jobs/inputs/job_2.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/works/job_2
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/jobs/inputs/job_3.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/works/job_3
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/jobs/inputs/job_4.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_1/works/job_4
maestro run next -t /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/flow.json -i 0 --dry-run
maestro run task -t /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/flow.json -i 1 --dry-run


maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/jobs/inputs/job_0.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/works/job_0
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/jobs/inputs/job_1.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/works/job_1
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/jobs/inputs/job_2.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/works/job_2
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/jobs/inputs/job_3.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/works/job_3
maestro run job -i /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/jobs/inputs/job_4.json -o /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/tasks/task_2/works/job_4
maestro run next -t /home/joao.pinto/git_repos/maestro-lightning/ci_test/local_tasks/flow.json -i 1 --dry-run
