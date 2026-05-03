#!/usr/bin/env python3

import typer
from maestro_lightning.runners.job_runner import run_job
from maestro_lightning.runners.task_runner import run_init, run_next
from .task import task_app, expert_app

app = typer.Typer(help="Maestro Lightning Orchestrator")
run_group = typer.Typer(help="Commands to run jobs and tasks")

# Register run subcommands
run_group.command("job", help="Run job runner.")(run_job)
run_group.command("task", help="Run the task init")(run_init)
run_group.command("next", help="Run the task finalizing")(run_next)

# Add groups to main app
app.add_typer(run_group, name="run")
app.add_typer(task_app, name="task")
app.add_typer(expert_app, name="expert")

def run():
    app()

if __name__ == "__main__":
    app()