__all__ = []

import os
import typer
from loguru import logger  
from tabulate import tabulate

from maestro_lightning.models.status import State
from maestro_lightning import get_context
from maestro_lightning import setup_logs
from maestro_lightning.models import Dataset, Image, Task, Context
from maestro_lightning.flow import load, print_tasks, Flow

def load_context(input_file: str, message_level: str, logname: str) -> Context:
    task_file = input_file + f"/flow.json"
    setup_logs(name=logname, level=message_level)
    ctx = get_context(clear=True)
    logger.info(f"Loading task file {task_file}.")
    load(task_file, ctx)
    return ctx

#
# common user functions
#

# 
# create task
#
def run_create(
    input_file: str = typer.Option(..., "-i", "--input", help="The job input file"),
    message_level: str = typer.Option("ERROR", "--message-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is ERROR."),
    command: str = typer.Option(..., "-c", "--command", help="The command to execute."),
    image: str = typer.Option(None, "-m", "--image", help="The image to use."),
    outputs: str = typer.Option(..., "-o", "--outputs", help="The outputs of the task."),
    partition: str = typer.Option(None, "-p", "--partition", help="The partition to use."),
    binds: str = typer.Option("{}", "-b", "--binds", help="The binds to use."),
    output_dir: str = typer.Option(..., "-d", "--output-dir", help="The output directory for the task."),
    name: str = typer.Option(..., "-n", "--name", help="The name of the task."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Perform a dry run without executing the task."),
):
    with Flow(name=name, path=output_dir) as session:
        Task(name=name,
             command=command,
             input_data= Dataset(name="input_dataset", path=input_file),
             outputs=eval(outputs),
             partition=partition,
             image=Image(name="image", path=image) if image else None,
             binds=eval(binds))
        session.run()


#
# list tasks
#
def run_list(
    input_file: str = typer.Option(..., "-i", "--input", help="The job input file"),
    message_level: str = typer.Option("ERROR", "--message-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is ERROR."),
):
    ctx = load_context(input_file, message_level, "task_list")
    print_tasks(ctx)  
    

#
# retry tasks
#
def run_retry(
    input_file: str = typer.Option(..., "-i", "--input", help="The job input file"),
    message_level: str = typer.Option("ERROR", "--message-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is ERROR."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Perform a dry run without executing the task."),
):
    ctx = load_context(input_file, message_level, "task_retry")
    logger.info(f"Retrying failed tasks in the flow: {ctx.path}")
    for task in ctx.tasks.values():
        if task.status != State.COMPLETED:
            for job in task.jobs:
                logger.info(f"Retrying job {job.job_id} of task {task.name}.")
                if job.status != State.COMPLETED:
                    job.status = State.ASSIGNED
            task.status=State.ASSIGNED
    
    for task in ctx.tasks.values():
        if len(task.prev) == 0:
            logger.info(f"Preparing task {task.name} for execution.")
            command = f"maestro run task -t {ctx.path}/flow.json -i {task.task_id}"
            command+=" --dry-run" if dry_run else ""
            print(command)
            os.system(command)
   
  
#
# expert functions
# 
    

#
# list jobs
#
def list_jobs(
    input_file: str = typer.Option(..., "-i", "--input", help="The job input file"),
    message_level: str = typer.Option("ERROR", "--message-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is ERROR."),
    filter_status: str = typer.Option(None, "--filter-status", help="A common separated list of status to be printed (e.g, failed,completed )."),
):
    ctx = load_context(input_file, message_level, "list_jobs")
    rows  = []
    for task in ctx.tasks.values():
        for job in task.jobs:
            status_list = filter_status.split(',') if filter_status else []
            row = [task.name, task.task_id ,job.job_id, job.status.value]
            ok = job.status.value in status_list if len(status_list)>0 else True
            if ok:
                rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)   
    
def change_jobs_status(
    input_file: str = typer.Option(..., "-i", "--input", help="The job input file"),
    message_level: str = typer.Option("ERROR", "--message-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is ERROR."),
    task_id: int = typer.Option(..., "--task-id", help="The ID of the task to change the job status."),
    from_status: str = typer.Option(..., "-f", "--from-status", help="The current status of the job to be changed."),
    to_status: str = typer.Option(..., "-t", "--to-status", help="The new status to set for the job."),
):
    ctx = load_context(input_file, message_level, "change_jobs_status")
    rows  = []
    for task in ctx.tasks.values():
        if task.task_id == task_id:
            for job in task.jobs:
                if job.status.value == from_status:
                    row = [task.name, task.task_id ,job.job_id, job.status.value]
                    job.status = State(to_status)
                    row.append(job.status.value)
                    rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'old_status', 'new_status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)     
    
def change_task_status(
    input_file: str = typer.Option(..., "-i", "--input", help="The job input file"),
    message_level: str = typer.Option("ERROR", "--message-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is ERROR."),
    task_id: int = typer.Option(..., "--task-id", help="The ID of the task to change the status."),
    new_status: str = typer.Option(..., "--new-status", help="The new status to set for the task."),
):
    ctx = load_context(input_file, message_level, "change_task_status")
    rows  = []
    for task in ctx.tasks.values():
        if task.task_id == task_id:
            row = [task.name, task.task_id ,task.status.value]
            task.status = State(new_status)
            row.append(task.status.value)
            rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'old_status', 'new_status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)  
              
def reset_task(
    input_file: str = typer.Option(..., "-i", "--input", help="The job input file"),
    message_level: str = typer.Option("ERROR", "--message-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is ERROR."),
    task_id: int = typer.Option(..., "--task-id", help="The ID of the task to be reset."),
    force: bool = typer.Option(False, "--force", help="Force reset even if the task is completed or finalized."),
    delete_workarea: bool = typer.Option(False, "--delete-workarea", help="Delete the workarea of the task."),
):
    task_file = input_file+f"/flow.json"
    setup_logs( name = f"reset_task", level=message_level )
    ctx = get_context( clear=True )
    logger.info(f"Loading task file {task_file}.")
    load( task_file , ctx)
    rows  = []
    
    for task in ctx.tasks.values():
        if task.task_id == task_id:
            if task.status in [State.COMPLETED,State.FINALIZED] and not force:
                message = f"Task {task.name} is in status {task.status.value}. Use --force to forcing reset."
                logger.warning(message)
                raise Exception(f"Task {task.name} is in status {task.status.value}. Use --force to forcing reset.")
            
            row = [task.name, task.task_id ,task.status.value]
            for job in task.jobs:
                job.status = State.ASSIGNED
            task.status = State.ASSIGNED
            row.append(task.status.value)
            rows.append(row)
            logger.info(f"clear job input file in {task.path}/jobs/inputs")
            os.system(f"rm -rf {task.path}/jobs/inputs/*")
            os.system(f"rm -rf {task.path}/jobs/status/*")
            if delete_workarea:
                os.system(f"rm -rf {task.path}/works/*")

    cols = ['taskname', 'task_id', 'old_status', 'new_status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)          