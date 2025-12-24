#!/usr/bin/env python3

import sys
import argparse

from maestro_lightning import get_argparser_formatter
from maestro_lightning.runners.job_runner import job_parser, run_job
from maestro_lightning.runners.task_runner import task_parser, run_init, run_next
from .task import list_parser, run_list, create_parser, retry_parser, run_create, run_retry, list_jobs, list_jobs_parser
from .task import change_jobs_status, change_jobs_status_parser, change_task_status_parser, change_task_status
from .task import reset_task, reset_task_parser

def build_argparser():

    formatter_class = get_argparser_formatter()

    parser    = argparse.ArgumentParser(formatter_class=formatter_class)
    mode = parser.add_subparsers(dest='mode')


    run_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = run_parent.add_subparsers(dest='option')
    option.add_parser("job" , parents = job_parser()   ,help='Run job runner.',formatter_class=formatter_class)
    option.add_parser("task", parents = task_parser()  ,help='Run the task init',formatter_class=formatter_class)
    option.add_parser("next", parents = task_parser()  ,help='Run the task finalizing',formatter_class=formatter_class)
    mode.add_parser( "run", parents=[run_parent], help="",formatter_class=formatter_class)
    
    
    
    task_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = task_parent.add_subparsers(dest='option')
    option.add_parser("create"   , parents = create_parser()    ,help='',formatter_class=formatter_class)
    option.add_parser("retry"    , parents = retry_parser()    ,help='',formatter_class=formatter_class)
    option.add_parser("list"   , parents = list_parser()    ,help='',formatter_class=formatter_class)
    mode.add_parser( "task", parents=[task_parent], help="",formatter_class=formatter_class)

    expert_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = expert_parent.add_subparsers(dest='option')
    option.add_parser("list-jobs"           , parents = list_jobs_parser()    ,help='',formatter_class=formatter_class)
    option.add_parser("change-jobs-status"  , parents = change_jobs_status_parser()    ,help='',formatter_class=formatter_class)
    option.add_parser("change-task-status"  , parents = change_task_status_parser()   ,help='',formatter_class=formatter_class)
    option.add_parser("reset-task"          , parents = reset_task_parser()   ,help='',formatter_class=formatter_class)
    mode.add_parser( "expert", parents=[expert_parent], help="",formatter_class=formatter_class)

    return parser

def run_parser(args):
    if args.mode == "run":
        if args.option == "job":
            run_job(args)
        elif args.option == "task":
            run_init(args)
        elif args.option == "next":
            run_next(args)
    elif args.mode == "task":
        if args.option == "list":
            run_list(args)
        elif args.option == "create":
            run_create(args)
        elif args.option == "retry":
            run_retry(args)

    elif args.mode == "expert":
        if args.option == "list-jobs":
            list_jobs(args)
        elif args.option == "change-job-status":
            change_jobs_status(args)
        elif args.option == "change-task-status":
            change_task_status(args)
        elif args.option == "reset-task":
            reset_task(args)

def run():

    parser = build_argparser()
    if len(sys.argv)==1:
        print(parser.print_help())
        sys.exit(1)

    args = parser.parse_args()
    run_parser(args)



if __name__ == "__main__":
  run()