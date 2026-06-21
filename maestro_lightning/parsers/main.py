#!/usr/bin/env python3

import typer

from maestro_lightning.runners.job_runner import run_job
from maestro_lightning.runners.task_runner import run_init, run_next
from .task import run_list, run_create, run_retry, list_jobs, change_jobs_status, change_task_status, reset_task

app = typer.Typer(help="Maestro CLI tool")
run_app = typer.Typer(help="Run jobs or tasks")
task_app = typer.Typer(help="Manage tasks")
expert_app = typer.Typer(help="Expert commands for tasks and jobs")

app.add_typer(run_app, name="run")
app.add_typer(task_app, name="task")
app.add_typer(expert_app, name="expert")


#
# Run App Commands
#

run_app.command("job", help="Run job runner.")(run_job)
run_app.command("task", help="Run the task init")(run_init)
run_app.command("next", help="Run the task finalizing")(run_next)


#
# Task App Commands
#

task_app.command("create", help="Create a task.")(run_create)
task_app.command("retry", help="Retry tasks.")(run_retry)
task_app.command("list", help="List tasks.")(run_list)


#
# Expert App Commands
#

expert_app.command("list-jobs", help="List jobs.")(list_jobs)
expert_app.command("change-jobs-status", help="Change jobs status.")(change_jobs_status)
expert_app.command("change-task-status", help="Change task status.")(change_task_status)
expert_app.command("reset-task", help="Reset task.")(reset_task)


def run():
    app()


if __name__ == "__main__":
    run()