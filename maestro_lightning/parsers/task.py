__all__ = []

import os
import argparse
from loguru import logger  
from tabulate import tabulate

from maestro_lightning.models.status import State
from maestro_lightning import get_context
from maestro_lightning import setup_logs
from maestro_lightning.models import Dataset, Image, Task, Context
from maestro_lightning.flow import load, print_tasks, Flow
from tabulate import tabulate

def load_context( args , logname ) -> Context:
    task_file = args.input_file+f"/flow.json"
    setup_logs( name = logname, level=args.message_level )
    ctx = get_context( clear=True )
    logger.info(f"Loading task file {task_file}.")
    load( task_file , ctx)
    return ctx

#
# common user functions
#


# 
# create task
#
def run_create(args):
    with Flow(name="flow", path=args.output_dir) as session:
        Task(name=args.name,
             command=args.command,
             input_data= Dataset(name="input_dataset", path=args.input_file),
             outputs=eval(args.outputs),
             partition=args.partition,
             image=Image(name="image", path=args.image) if args.image else None,
             binds=eval(args.binds))
        session.run()


#
# list tasks
#
def run_list(args):
    ctx = load_context(args, "task_list")
    print_tasks(ctx)  
    

#
# retry tasks
#
def run_retry(args):
    ctx = load_context(args, "task_retry")
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
            command+=" --dry-run" if args.dry_run else ""
            print(command)
            os.system(command)
   
  
#
# expert functions
# 
    

#
# list jobs
#
def list_jobs( args ):
    ctx = load_context(args, "list_jobs")
    rows  = []
    for task in ctx.tasks.values():
        for job in task.jobs:
            status_list = args.filter_status.split(',') if args.filter_status else []
            row = [task.name, task.task_id ,job.job_id, job.status.value]
            ok = job.status.value in status_list if len(status_list)>0 else True
            if ok:
                rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)   
    
def change_jobs_status( args ):
    ctx = load_context(args, "change_jobs_status")
    rows  = []
    for task in ctx.tasks.values():
        if task.task_id == args.task_id:
            for job in task.jobs:
                if job.status.value == args.from_status:
                    row = [task.name, task.task_id ,job.job_id, job.status.value]
                    job.status = State(args.to_status)
                    row.append(job.status.value)
                    rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'old_status', 'new_status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)     
    
def change_task_status( args ):
    ctx = load_context(args, "change_task_status")
    rows  = []
    for task in ctx.tasks.values():
        if task.task_id == args.task_id:
            row = [task.name, task.task_id ,task.status.value]
            task.status = State(args.new_status)
            row.append(task.status.value)
            rows.append(row)
    cols = ['taskname', 'task_id', 'job_id', 'old_status', 'new_status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)  
              
def reset_task( args ):
    task_file = args.input_file+f"/flow.json"
    setup_logs( name = f"reset_task", level=args.message_level )
    ctx = get_context( clear=True )
    logger.info(f"Loading task file {task_file}.")
    load( task_file , ctx)
    rows  = []
    
    
    
    for task in ctx.tasks.values():
        if task.task_id == args.task_id:
            if task.status in [State.COMPLETED,State.FINALIZED] and not args.force:
                message = f"Task {task.name} is in status {task.status.value}. Use --forse to forcing reset."
                logger.warning(message)
                raise Exception(f"Task {task.name} is in status {task.status.value}. Use --forse to forcing reset.")
            
            row = [task.name, task.task_id ,task.status.value]
            for job in task.jobs:
                job.status = State.ASSIGNED
            task.status = State.ASSIGNED
            row.append(task.status.value)
            rows.append(row)
            logger.info(f"clear job input file in {task.path}/jobs/inputs")
            os.system(f"rm -rf {task.path}/jobs/inputs/*")
            os.system(f"rm -rf {task.path}/jobs/status/*")
            if args.delete_workarea:
                os.system(f"rm -rf {task.path}/works/*")

    cols = ['taskname', 'task_id', 'old_status', 'new_status']
    table = tabulate(rows ,headers=cols, tablefmt="psql")
    print(table)          
    

   

#
# user parsers
# 

def common_parser():
    parser = argparse.ArgumentParser(description = '', add_help = False)

    parser.add_argument('-i','--input', action='store', dest='input_file', required = True,
                        help = "The job input file")
    parser.add_argument("--message-level", action="store", dest="message_level", default="ERROR"
                        , help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is INFO.")
    return parser
    
def create_parser():
    parser = common_parser()
    parser.add_argument("-c", "--command", action="store", dest="command", required=True,
                        help="The command to execute.")
    parser.add_argument("-m", "--image", action="store", dest="image", required=False, default=None,
                        help="The image to use.")
    parser.add_argument("-o", "--outputs", action="store", dest="outputs", required=True,
                        help="The outputs of the task.")
    parser.add_argument("-p", "--partition", action="store", dest="partition", required=False,
                        help="The partition to use.")
    parser.add_argument("-b", "--binds", action="store", dest="binds", required=False, default="{}",
                        help="The binds to use.")
    parser.add_argument("-d", "--output-dir", action="store", dest="output_dir", required=True,
                        help="The output directory for the task.")
    parser.add_argument("-n", "--name", action="store", dest="name", required=True,
                        help="The name of the task.")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run", default=False,
                        help="Perform a dry run without executing the task.")
    return [parser] 
    
def list_parser():
    return [common_parser()]

def retry_parser():
    parser = common_parser()
    parser.add_argument("--dry-run", action="store_true", dest="dry_run", default=False,
                        help="Perform a dry run without executing the task.")
    return [parser]


#
# expert parsers
#

def list_jobs_parser():
    parser = common_parser()
    parser.add_argument("--filter-status", action="store", dest="filter_status", required=False, default=None,
                        help="A common separated list of status to be printed (e.g, failed,completed ).")
    return [parser]

def change_jobs_status_parser():
    parser = common_parser()
    parser.add_argument("--task-id", action="store", dest="task_id", required=True, type=int,
                        help="The ID of the task to change the job status.")
    parser.add_argument('-f', "--from-status", action="store", dest="from_status", required=True, type=str,
                        help="The current status of the job to be changed.")
    parser.add_argument('-t',"--to-status", action="store", dest="to_status", required=True, type=str,
                        help="The new status to set for the job.")
    return [parser]

def change_task_status_parser():
    parser = common_parser()
    parser.add_argument("--task-id", action="store", dest="task_id", required=True, type=int,
                        help="The ID of the task to change the status.")
    parser.add_argument("--new-status", action="store", dest="new_status", required=True, type=str,
                        help="The new status to set for the task.")
    return [parser]

def reset_task_parser():
    parser = common_parser()
    parser.add_argument("--task-id", action="store", dest="task_id", required=True, type=int,
                        help="The ID of the task to be reset.")
    parser.add_argument("--force", action="store_true", dest="force", default=False,
                        help="Force reset even if the task is completed or finalized.")
    parser.add_argument("--delete-workarea", action="store_true", dest="delete_workarea", default=False,
                        help="Delete the workarea of the task.")
    return [parser]