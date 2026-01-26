# Protein Pipeline - Distributed Data Analysis System

This repository contains a distributed protein analysis system deployed on a small cluster consisting of one `controller-node` and four `worker-node` instances. Developed for the UCL module COMP0104 – Engineering for Data Analysis, the system follows a controller/worker architecture and uses Celery, RabbitMQ, and Redis as its core backend components.

The system is designed to:
- distribute protein analysis tasks across multiple workers,
- track execution progress and results centrally on the controller,
- generate aggregated CSV outputs for each run,
- support monitoring and inspection during execution.

## 1. Provision the cluster
### 1.1 Install prerequisites 
On your local machine or VM (used to provision the mini-cluster)
- Git
- Terraform

### 1.2 Clone the repository (local)
```
git clone <gitlab-repo-url>
cd eda-cw
```

Repository structure:
```
eda-cw/
├── ansible/          # configuration & service setup
├── build_cluster/    # terraform infrastructure
├── code/             # protein pipeline code + UI
├── README.md
└── .gitignore
```

### 1.3 Execute Terraform
From your local machine:
```
cd build_cluster
terraform init
terraform apply
```
This creates:
- 1 `controller-node` (host) VM
- 4 `worker-node` VMs

### 1.4 SSH into `controller-node` (host)
```
ssh -A -J condenser almalinux@<controller-node> -i ~/.ssh/key
```

The -A flag enables SSH agent forwarding, allowing the `controller-node` to authenticate using SSH keys loaded in your local SSH agent (e.g., for Git authentication).

### 1.5 Install prerequisites
On `controller-node` (used to configure the mini-cluster)
- Git

### 1.6 Clone the repository (`controller-node`)
From the `controller-node`:
```
cd ~
git clone <gitlab-repo-url>
cd eda-cw
```

### 1.7 Generate inventory locally and scp to `controller-node`
From your local machine (where `terraform apply` was run):
```
cd ~/eda-cw/build_cluster
python ./generate_inventory.py >> inventory.json
scp -J condenser inventory.json almalinux@<controller-node>:/home/almalinux/eda-cw/ansible -i ~/.ssh/key
```

## 2. Configure the cluster
### Run Ansible playbooks

From the `controller-node`:
```
cd ~/eda-cw/ansible
ansible-playbook playbooks/controller_base.yml
ansible-playbook playbooks/workers_base.yml
```

> **Note:** An `ansible.cfg` file is provided in the `ansible/` directory.
> By default it:
> - uses a locally generated inventory (requires inventory.json to be present),
> - disables host key checking,
> - specifies a SSH private key for cluster access.
>
> Update the `private_key_file` entry in `ansible/ansible.cfg` before running the playbooks.


## 3. Running the pipeline
### 3.1 Inputs
By default, the pipeline operates on the *mouse proteome* FASTA and `experiment_ids.txt` provided in the coursework tarball.

#### Defaults
- The FASTA file `UP000000589_10090.fasta` is fetched during configuration via the worker base Ansible playbook,
- this FASTA is converted into a SQLite database (`proteome.db`) using: `~/protein_pipeline/code/build_proteome_db.py`,
- each worker queries this local SQLite database when processing proteins,
- protein IDs to analyse are read from: `~/protein_pipeline/code/experiment_ids.txt`.

#### Using a custom FASTA
The system supports swapping the input proteome **without modifying the pipeline code**.

*Workflow*:
1. `scp` a custom FASTA file to the controller at `~/protein_pipeline/data/custom.fasta`
2. Run the Ansible playbook:
```
cd ~/eda-cw/ansible
ansible-playbook playbooks/proteome_custom.yml
```
This will:
- distribute the FASTA to all workers,
- rebuild the database locally on each worker,
- restart Celery workers to ensure the new database is used.

#### Reverting to the default proteome
To restore the original mouse proteome:
```
cd ~/eda-cw/ansible
ansible-playbook playbooks/proteome_revert.yml
```
This copies a preserved snapshot (`mouse_proteome.backup.db`) back into place and restarts workers.

> **Note:** After switching proteomes, ensure `experiment_ids.txt` contains valid protein IDs for the selected FASTA.

### 3.2 Submit a demo run
On the `controller-node`:
```
cd ~/protein_pipeline
pp-runctl submit --limit 2
```
This command prepares a run using the first *N* protein IDs from `experiment_ids.txt`, creates a run specification stored in Redis, and dispatches the corresponding analysis tasks to the `worker-node` instances.

Internally, submission consists of separate `prepare` and `execute` steps; see:
`~/protein_pipeline/code/runctl.py`

### 3.3 Submit full experiment_ids.txt
```
pp-runctl submit
```
This submits a full run using all protein IDs listed in `experiment_ids.txt` and distributes the workload across all available `worker-node` instances.

### 3.4 Output files
When a run completes, results are written to:
```
~/protein_pipeline/runs/<run_id>
```

Files produced:
- `selected_ids.txt` (submitted protein ids),
- `spec.json` (run metadata),
- `hits_output.csv` (best hit per protein),
- `profile_output.csv` (aggregated statistics),
- `failures_output.csv` (failed proteins and error messages).

These files are also downloadable via the Streamlit interface.

## 4. Monitoring and Interfaces

The system exposes two web-based interfaces for monitoring execution and inspecting results.

### 4.1 Prometheus
Used to monitor system-level metrics and service health across the `controller-node` and `worker-node` instances.

> Access URL:
https://prometheus-ucabtg2.comp0235.condenser.arc.ucl.ac.uk

### 4.2 Streamlit Protein Monitoring Interface
A Streamlit-based interface provides run-level visibility into the protein pipeline.

> Access URL:
https://protein-pipeline-ucabtg2.comp0235.condenser.arc.ucl.ac.uk

UI allows you to:
- view all submitted runs,
- inspect per-run progress (submitted / running / succeeded / failed),
- download output files:
    - `hits_output.csv`,
    - `profile_output.csv`,
    - `failures_output.csv`,
- inspect individual protein results and errors.

## 5. Troubleshooting (quick reference)
The following commands are intended as a quick diagnostic reference.
Note that **different services run on different node types**:
- the `controller-node` runs Redis, RabbitMQ, the controller Celery worker, and the UI,
- the `worker-node` instances run only the Celery worker service that executes protein analyses (and node-exporter).

Run each command on the node where the corresponding service is expected to be active.

#### Check service status
```
systemctl status redis
systemctl status rabbitmq-server
systemctl status celery-worker
systemctl status redis celery-ctl-worker
```

#### Check logs
```
journalctl -u celery-worker
journalctl -u celery-ctl-worker
journalctl -u redis
journalctl -u rabbitmq-server
```

#### Check queues
```
sudo rabbitmqctl list_queues -p protein
```

#### Check Redis state
Export the required Redis connection variables:
```
REDIS_HOST=controller-node
REDIS_PORT=6379
REDIS_PASSWORD=imaproteinpipelinerunner
REDIS_DB=1
```
Then inspect run-level state:
```
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" -n "$REDIS_DB" HGETALL "pp:run:<run_id>"
```

To inspect a specific protein within a run:
```
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" -n "$REDIS_DB" HGETALL "pp:run:<run_id>:protein:<protein_id>"
```