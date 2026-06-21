__all__ = []

import typer
try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated
from loguru import logger
from maestro_lightning import State, get_context 
from maestro_lightning import sbatch, setup_logs 
from maestro_lightning.flow import load
from maestro_lightning.models import Task

def run_init(
    index           : Annotated[int, typer.Option("--index", "-i", help="The task index")],
    task_file       : Annotated[str, typer.Option("--task-file", "-t", help="The task file input")],
    message_level   : Annotated[str, typer.Option("--message-level", help="The logging message level")] = "INFO",
    dry_run         : Annotated[bool, typer.Option("--dry-run", help="Perform a dry run without executing any tasks.")] = False
):
    """
    Initialize a task.
    """
    setup_logs(name=f"task_runner:{index}", level=message_level)
    ctx = get_context(clear=True)
    logger.info(f"Initializing task with index {index}.")
    logger.info(f"Loading task file {task_file}.")
    load(task_file, ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get(index)
    
    partition = ctx["partition"]
    virtualenv = ctx["virtualenv"]
    condaenv = ctx["condaenv"]
    slurm_ops = {
        "OUTPUT_FILE": f"{task.path}/logs/task_end_{task.task_id}.out",
        "ERROR_FILE": f"{task.path}/logs/task_end_{task.task_id}.err",
        "JOB_NAME": f"next-{task.task_id}",
        "PARTITION": partition,
    }
    
    if task.has_jobs():
        logger.info(f"Fetched task {task.name} for initialization.")
        task.status = State.RUNNING  
        # create the main script
        logger.info(f"Submitting main script for task {task.name}.")
        if task.has_jobs():
            job_id = task.submit(dry_run=dry_run)
            logger.info(f"Submitted task {task.name} with job ID {job_id}.")
            slurm_ops["DEPENDENCY"] = f"afterok:{job_id}"
    else:
        logger.info(f"Task {task.name} already completed. Skipping initialization.")

    # create the closing script
    logger.info(f"Creating closing script for task {task.name}.")
    script = sbatch(f"{task.path}/scripts/close_task_{task.task_id}.sh", 
                     opts=slurm_ops, 
                     virtualenv=virtualenv, 
                     condaenv=condaenv
                    )    
    command = f"maestro run next -t {ctx.path}/flow.json -i {task.task_id}"
    script += command
    logger.info(f"Submitting closing script for task {task.name}.")
    print(command)
    if not dry_run:
        script.submit()

def run_next(
    index           : Annotated[int, typer.Option("--index", "-i", help="The task index")],
    task_file       : Annotated[str, typer.Option("--task-file", "-t", help="The task file input")],
    message_level   : Annotated[str, typer.Option("--message-level", help="The logging message level")] = "INFO",
    dry_run         : Annotated[bool, typer.Option("--dry-run", help="Perform a dry run without executing any tasks.")] = False
):
    """
    Finalize a task.
    """
    setup_logs(name=f"TaskCloser:{index}", level=message_level)
    ctx = get_context(clear=True)
    logger.info(f"Finalizing task with index {index}.")
    logger.info(f"Loading task file {task_file}.")
    load(task_file, ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get(index)
    logger.info(f"Fetched task {task.name} for finalization.")
    
    # update task status 
    job_status = [job.status for job in task.jobs]
    if all([status == State.COMPLETED for status in job_status]):
        logger.info(f"All jobs for task {task.name} completed successfully.")
        task.status = State.COMPLETED
    elif sum([status == State.FAILED for status in job_status]) / len(job_status) > 0.1:
        logger.info(f"More than 10% of jobs for task {task.name} failed.")
        task.status = State.FAILED

        def cancel_task(task: Task):
            for next_task in task.next:
                logger.info(f"Canceling dependent task {next_task.name}.")
                next_task.status = State.CANCELED
                cancel_task(next_task)
        logger.info(f"Task {task.name} failed. Canceling dependent tasks.")
        if not dry_run:
            cancel_task(task)      
    else:
        logger.info(f"Some jobs for task {task.name} failed, but within acceptable limits.")
        task.status = State.FINALIZED
        
    # if the current task is failed, we need to cancel the entire graph
    if task.status in [State.COMPLETED, State.FINALIZED]:
        logger.info(f"Task {task.name} finalized successfully.")
        # need to start the other tasks that depend on this one
        for task in task.next:
            partition = ctx["partition"]
            virtualenv = ctx["virtualenv"]  
            condaenv = ctx["condaenv"]
            slurm_opts = {
                "OUTPUT_FILE": f"{task.path}/logs/task_begin_{task.task_id}.out",
                "ERROR_FILE": f"{task.path}/logs/task_begin_{task.task_id}.err",
                "JOB_NAME": f"init-{task.task_id}",
                "PARTITION": partition,
            }
            logger.info(f"Starting dependent task {task.name}.")
            script = sbatch(f"{task.path}/scripts/init_task_{task.task_id}.sh", 
                             opts=slurm_opts, 
                             virtualenv=virtualenv, 
                             condaenv=condaenv
                            )
            command = f"maestro run task -t {ctx.path}/flow.json -i {task.task_id}"
            script += command
            print(command)
            logger.info(f"Submitting initialization script for task {task.name}.")
            if not dry_run:
                script.submit()
