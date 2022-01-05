<!--
Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

In applying this licence, ECMWF does not waive the privileges and immunities
granted to it by virtue of its status as an intergovernmental organisation nor
does it submit to any jurisdiction.
-->

# Polytope administration client

This repository contains the source code and documentation of a Polytope admin client implemented in Python, which communicates with the RESTful API exposed by a Polytope server. It includes commands for user creation and deletion, among others.

&nbsp;
## 1. Installation

Install the Polytope admin client with python3 (>= 3.6) and pip as follows:
```bash
python3 -m pip install --upgrade ./path/to/polytope/admin/polytope-admin
# make sure the installed polytope executable is added to your PATH if willing to use the CLI
```

&nbsp;
## 2. API example

```bash
export POLYTOPE_USERNAME=<admin_account_name>
export POLYTOPE_PASSWORD=<admin_account_password>
```

```python
#!/usr/bin/env python3

from polytope_admin.api import Client

help(Client)

c = Client()

help(c)

c.create_user('johndoe')

c.delete_user('johndoe')
```

&nbsp;
## 3. CLI example

You can check the documentation of the CLI as follows.
```bash
polytope-admin -h
```

```bash
export POLYTOPE_USERNAME=<admin_account_name>
export POLYTOPE_PASSWORD=<admin_account_password>

polytope-admin list config

polytope-admin create user johndoe

polytope-admin delete user johndoe
```

The following dialog shows an overview of the syntax of the CLI:
```bash

# High-level user commands

# global options:
# 
# -c --config-path
# -a --address
# -p --port
# -u --username
# -k --key
# -v --verbose
# --log-file
# --log-level


polytope-admin set config <key> <value> [--global-opt value ...]

polytope-admin unset config <key> | all [--global opts ...]

polytope-admin list config [--global opts ...]



polytope-admin create user <name> [-p|--password <pwd>] [--affiliation <aff>] 
                            [--soft-limit <sl>] [--hard-limit <hl>] [--global-opts ...]

polytope-admin delete user <name> [--global-opts ...]

polytope-admin login [--login-password] [--key-type] [--global-opts ...]
```
