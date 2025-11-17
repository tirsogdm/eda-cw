#!/usr/bin/env python3

import json
import sys 
from subprocess import run, PIPE
from pathlib import Path

def generate_inventory():
    # Relative path to Terraform directory
    script_dir = Path(__file__).resolve().parent
    tf_dir = script_dir.parent

    # 1) Get terraform outputs
    cmd = ["terraform", f"-chdir={tf_dir}", "output", "--json"]
    out = run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(out.stdout)

    worker_ips = data["worker_ips"]["value"]
    mgmt_ips = data["host_ips"]["value"]

    # TESTING -- remove line
    worker_ips = [ip for ip in worker_ips if ip in ["10.134.12.200"]]

    hostvars = {}
    groups = {}

    # Mgmt group
    mgmt_name = "mgmt-node"
    hostvars[mgmt_name] = {
        "ansible_host": mgmt_ips[0],
        "ansible_user": "almalinux",
    }
    groups["mgmt"] = {"hosts": [mgmt_name]}

    # Workers group
    worker_names = []
    for i, ip in enumerate(worker_ips, start=1):
        name = f"worker-node{i}"
        worker_names.append(name)
        hostvars[name] = {
            "ansible_host": ip,
            "ansible_user": "almalinux",
        }
    groups["workers"] = {"hosts": worker_names}

    # All group
    groups["all"] = {"children": ["mgmt", "workers"]}

    inventory = {
        "_meta": {"hostvars": hostvars},
        **groups,
    }

    return json.dumps(inventory, indent=4)

if __name__ == "__main__":
    inv = generate_inventory()
    print(inv)