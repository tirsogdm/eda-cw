#!/usr/bin/env python3
import sys, json

with open("inventory.json", "r") as f:
    inventory = json.load(f)

if "--list" in sys.argv:
    print(json.dumps(inventory))
elif "--host" in sys.argv:
    print(json.dumps({}))
else:
    print(json.dumps(inventory))