
[![maestro](https://github.com/lps-ufrj-br/maestro-lightning/actions/workflows/flow.yml/badge.svg)](https://github.com/lps-ufrj-br/maestro-lighning/actions/workflows/flow.yml)


# Maestro Lightning 🚀

Maestro Lightning is a repository to manage workloads and jobs in SLURM using a Pythonic approach. 

## Features ✨
- Create tasks 🛠️
- Connect tasks in sequence 🔗
- Add datasets 📊
- Link images 🖼️

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


### Create a simple job:
```python
import os, json
basepath = os.getcwd()
input_path = f"{basepath}/input_data"
os.makedirs(input_path, exist_ok=True)
for i in range(20):
    with open(f"{input_path}/{i}.json",'w') as f:
        d={'a':i*10,'b':i*2}
        json.dump(d,f)
```

### Create a simple flow:
```python
from maestro_lightning import Flow, Task, Dataset, Image

with Flow(name="local_provider", path=f"{basepath}/local_tasks") as session:
    input_dataset    = Dataset(name="input_data", path=f"{basepath}/input_data")
    image            = Image(name="python", path=f"{basepath}/python3.10.sif")
    command          = f"python3 {basepath}/app.py --job %IN --output %OUT"
    task_1 = Task(name="example_task_1",
                  image=image,
                  command=command,
                  input_data=input_dataset,
                  outputs={'OUT':'output.json'},
                  partition='cpu-large',
                  )
    session.run()
```