# Overview

[fabric](http://docs.fabfile.org/en/latest/index.html) scripts for deploying spark-jobserver to DSE cluster machines.

# Setup

$ ./setup.sh
$ source env/bin/activate

## List available commands

(env)$ fab -l

# Fresh install

For a first-time install to a node in the cluster, including one-time setup items:

(env)$ fab full_deploy

# Stop/start jobserver on a node

(env)$ fab stop_server
(env)$ fab start_server

# Deploy jobserver code changes

To push out new jobserver code:

(env)$ fab deploy_code