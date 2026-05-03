__all__ = ["task_app", "expert_app"]

import os
import typer
try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated
from typing import Optional
from loguru import logger  
from tabulate import tabulate

from maestro_lightning.models.status import State
from maestro_lightning import get_context
from maestro_lightning import setup_logs
from maestro_lightning.models import Dataset, Image, Task, Context
from maestro_lightning.flow import load, print_tasks, Flow

task_app = typer.Typer(help="Task management commands")
expert_app = typer.Typer(help="Expert management commands")

def load_context(input_file: str, message_level: str, logname: str) -> Context:
    task_file = os.path.join(input_file, "flow.json")
    setup_logs(name=logname, level=message_level)
    ctx = get_context(clear=True)
    logger.info(f"Loading task file {task_file}.")
    load(task_file, ctx)
    return ctx

#
# task commands
#

@task_app.command("create")
def run_create(
    input_file      : Annotated[str, typer.Option("--input", "-i", help="The job input file")],
    output_dir      : Annotated[str, typer.Option("--output-dir", "-d", help="The output directory for the task")],
    name            : Annotated[str, typer.Option("--name", "-n", help="The name of the task")],
    command         : Annotated[str, typer.Option("--command", "-c", help="The command to execute")],
    outputs         : Annotated[str, typer.Option("--outputs", "-o", help="The outputs of the task")],
    image           : Annotated[Optional[str], typer.Option("--image", "-m", help="The image to use")] = None,
    partition       : Annotated[Optional[str], typer.Option("--partition", "-p", help="The partition to use")] = None,
    binds           : Annotated[str, typer.Option("--binds", "-b", help="The binds to use")] = "{}",
    message_level   : Annotated[str, typer.Option("--message-level", help="Set the logging level")] = "ERROR",
    dry_run         : Annotated[bool, typer.Option("--dry-run", help="Perform a dry run without executing the task")] = False
):
    """
    Create a new task.
    """
    with Flow(name="flow", path=output_dir) as session:
        Task(name=name,
             command=command,
             input_data=Dataset(name="input_dataset", path=input_file),
             outputs=eval(outputs),
             partition=partition,
             image=Image(name="image", path=image) if image else None,
             binds=eval(binds))
        session.run()

@task_app.command("list")
def run_list(
    input_file      : Annotated[str, typer.Option("--input", "-i", help="The job input file")],
    message_level   : Annotated[str, typer.Option("--message-level", help="Set the logging level")] = "ERROR"
):
    """
    List tasks in the flow.
    """
    ctx = load_context(input_file, message_level, "task_list")
    print_tasks(ctx)  

@task_app.command("retry")
def run_retry(
    input_file      : Annotated[str, typer.Option("--input", "-i", help="The job input file")],
    message_level   : Annotated[str, typer.Option("--message-level", help="Set the logging level")] = "ERROR",
    dry_run         : Annotated[bool, typer.Option("--dry-run", help="Perform a dry run without executing the task")] = False
):
    """
    Retry failed tasks in the flow.
    """
    ctx = load_context(input_file, message_level, "task_retry")
    logger.info(f"Retrying failed tasks in the flow: {ctx.path}")
    for task in ctx.tasks.values():
        if task.status != State.COMPLETED:
            for job in task.jobs:
                logger.info(f"Retrying job {job.job_id} of task {task.name}.")
                if job.status != State.COMPLETED:
                    job.status = State.ASSIGNED
            task.status = State.ASSIGNED
    
    for task in ctx.tasks.values():
        if len(task.prev) == 0:
            logger.info(f"Preparing task {task.name} for execution.")
            command = f"maestro run task -t {ctx.path}/flow.json -i {task.task_id}"
            command += " --dry-run" if dry_run else ""
            print(command)
            os.system(command)

#
# expert commands
#

@expert_app.command("list-jobs")
def list_jobs(
    input_file      : Annotated[str, typer.Option("--input", "-i", help="The job input file")],
    message_level   : Annotated[str, typer.Option("--message-level", help="Set the logging level")] = "ERROR",
    filter_status   : Annotated[Optional[str], typer.Option("--filter-status", help="A comma separated list of status to be printed (e.g, failed,completed ).")] = None
):
    """
    List jobs for tasks.
    """
    ctx = load_context(input_file, message_level, "list_jobs")
    rows = []
    for task in ctx.tasks.values():
        for job in task.jobs:
            status_list = filter_status.split(',') if filter_status else []
            row = [task.name, task.task_id, job.job_id, job.status.value]
            ok = job.status.value in status_list if len(status_list) > 0 else True
            if ok:
                rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'status']
    table = tabulate(rows, headers=cols, tablefmt="psql")
    print(table)   

@expert_app.command("change-jobs-status")
def change_jobs_status(
    input_file      : Annotated[str, typer.Option("--input", "-i", help="The job input file")],
    task_id         : Annotated[int, typer.Option("--task-id", help="The ID of the task to change the job status.")],
    from_status     : Annotated[str, typer.Option("--from-status", "-f", help="The current status of the job to be changed.")],
    to_status       : Annotated[str, typer.Option("--to-status", "-t", help="The new status to set for the job.")],
    message_level   : Annotated[str, typer.Option("--message-level", help="Set the logging level")] = "ERROR",
):
    """
    Change the status of jobs in a task.
    """
    ctx = load_context(input_file, message_level, "change_jobs_status")
    rows = []
    for task in ctx.tasks.values():
        if task.task_id == task_id:
            for job in task.jobs:
                if job.status.value == from_status:
                    row = [task.name, task.task_id, job.job_id, job.status.value]
                    job.status = State(to_status)
                    row.append(job.status.value)
                    rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'old_status', 'new_status']
    table = tabulate(rows, headers=cols, tablefmt="psql")
    print(table)     

@expert_app.command("change-task-status")
def change_task_status(
    input_file      : Annotated[str, typer.Option("--input", "-i", help="The job input file")],
    task_id         : Annotated[int, typer.Option("--task-id", help="The ID of the task to change the status.")],
    new_status      : Annotated[str, typer.Option("--new-status", help="The new status to set for the task.")],
    message_level   : Annotated[str, typer.Option("--message-level", help="Set the logging level")] = "ERROR"
):
    """
    Change the status of a task.
    """
    ctx = load_context(input_file, message_level, "change_task_status")
    rows = []
    for task in ctx.tasks.values():
        if task.task_id == task_id:
            row = [task.name, task.task_id, task.status.value]
            task.status = State(new_status)
            row.append(task.status.value)
            rows.append(row)
    cols = ['taskname', 'task_id', 'old_status', 'new_status']
    table = tabulate(rows, headers=cols, tablefmt="psql")
    print(table)  

@expert_app.command("reset-task")
def reset_task(
    input_file      : Annotated[str, typer.Option("--input", "-i", help="The job input file")],
    task_id         : Annotated[int, typer.Option("--task-id", help="The ID of the task to be reset.")],
    force           : Annotated[bool, typer.Option("--force", help="Force reset even if the task is completed or finalized.")] = False,
    delete_workarea : Annotated[bool, typer.Option("--delete-workarea", help="Delete the workarea of the task.")] = False,
    message_level   : Annotated[str, typer.Option("--message-level", help="Set the logging level")] = "ERROR"
):
    """
    Reset a task and its jobs.
    """
    ctx = load_context(input_file, message_level, "reset_task")
    rows = []
    for task in ctx.tasks.values():
        if task.task_id == task_id:
            if task.status in [State.COMPLETED, State.FINALIZED] and not force:
                message = f"Task {task.name} is in status {task.status.value}. Use --force for forcing reset."
                logger.warning(message)
                raise Exception(message)
            
            row = [task.name, task.task_id, task.status.value]
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
    table = tabulate(rows, headers=cols, tablefmt="psql")
    print(table)