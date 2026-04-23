
[![maestro](https://github.com/lps-ufrj-br/maestro-lightning/actions/workflows/flow.yml/badge.svg)](https://github.com/lps-ufrj-br/maestro-lightning/actions/workflows/flow.yml)
[![pages](https://github.com/lps-ufrj-br/maestro-lightning/actions/workflows/pages.yml/badge.svg)](https://github.com/lps-ufrj-br/maestro-lightning/actions/workflows/pages.yml)

# Maestro Lightning ([WebPage](https://lps-ufrj-br.github.io/maestro-lightning/))🚀

## 🧪 CI Status

| Test Job | Target | Status |
| :--- | :--- | :--- |
| **Environment** | Python Build & Setup | ![Build](https://img.shields.io/github/actions/workflow/status/lps-ufrj-br/maestro-lightning/flow.yml?job=build&label=build&branch=main) |
| **Flow** | Maestro Flow Test | ![Flow](https://img.shields.io/github/actions/workflow/status/lps-ufrj-br/maestro-lightning/flow.yml?job=flow&label=flow&branch=main) |
| **Task 1** | Creation & Run | ![Task 1](https://img.shields.io/github/actions/workflow/status/lps-ufrj-br/maestro-lightning/flow.yml?job=run_task_one&label=task1&branch=main) |
| **Task 2** | Creation & Run | ![Task 2](https://img.shields.io/github/actions/workflow/status/lps-ufrj-br/maestro-lightning/flow.yml?job=run_task_two&label=task2&branch=main) |
| **Documentation** | GitHub Pages Build | ![Pages](https://img.shields.io/github/actions/workflow/status/lps-ufrj-br/maestro-lightning/pages.yml?job=build&label=pages&branch=main) |

---

Maestro Lightning is a repository to manage workloads and jobs in SLURM using a Pythonic approach. 

## Features ✨
- Create tasks 🛠️
- Connect tasks in sequence 🔗
- Add datasets 📊
- Link images 🖼️

## Requirements:

- SLURM cluster
- Singularity
- Virtual environment

## Installation 📦

To install Maestro Lightning from scrath:
```bash
source activate.sh
```

Or install using pip package:

```
pip install maestro-lightning
```


## 🐍 Example Usage

To create and run a simple workflow using Maestro Lightning, follow the example below:

### Create a simple flow:
```python
from maestro_lightning import Flow, Task, Dataset, Image

with Flow(name="local_provider", path=f"{basepath}/local_tasks") as session:
    input_dataset    = Dataset(name="input_data", path="path/to/jobs")
    image            = Image(name="python", path=f"/path/to/image.sif")
    command          = f"python3 /path/to/script.py --job %IN --output %OUT"
    task_1 = Task(name="example_task_1",
                  image=image,
                  command=command,
                  input_data=input_dataset,
                  outputs={'OUT':'output.json'},
                  partition='cpu-large',
                  )
    session.run()
```

### How to run a large sequence?

* [Reconstruction sequence for HEP problems](docs/How_to_run_the_sequence.html)
