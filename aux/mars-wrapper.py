#!/usr/bin/env python
#
# Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#


import logging
import os
import subprocess
import sys

import requests

port_file = "/persistent/last_mars_port"


def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    assert len(sys.argv) == 2

    # Read Kubernetes service account details for authentication
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token", "r") as file:
        token = file.read().strip()
    headers = {"Authorization": "Bearer " + token}

    # Set the MARS client environment variables
    node_name = os.environ["K8S_NODE_NAME"]
    pod_name = os.environ["K8S_POD_NAME"]  # = service name
    namespace = os.environ["K8S_NAMESPACE"]

    service_url = (
        f"https://{os.environ['KUBERNETES_SERVICE_HOST']}:"
        f"{os.environ['KUBERNETES_PORT_443_TCP_PORT']}/api/v1/namespaces/"
        f"{namespace}/services/{pod_name}"
    )
    response = requests.get(
        service_url,
        headers=headers,
        verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    )
    response.raise_for_status()  # Raise an exception for HTTP errors
    service = response.json()["spec"]

    try:
        with open(port_file, "rt") as f:
            last_port_id = int(f.read())
    except FileNotFoundError:
        last_port_id = 0

    port_id = (last_port_id + 1) % 5

    with open(port_file, "w+") as f:
        f.write(str(port_id))

    node_port = service["ports"][port_id]["nodePort"]
    local_port = service["ports"][port_id]["port"]

    logging.info("Callback on {}:{}".format(node_name, node_port))

    env = {
        **os.environ,
        "MARS_ENVIRON_ORIGIN": "polytope",
        "MARS_DHS_CALLBACK_HOST": node_name,
        "MARS_DHS_CALLBACK_PORT": str(node_port),
        "MARS_DHS_LOCALPORT": str(local_port),
        "MARS_DHS_LOCALHOST": pod_name,
        # "MARS_DEBUG": str(1),
        # "ECKIT_DEBUG": str(1),
        # "FDB_DEBUG": str(1),
    }

    # Call MARS
    mars_command = os.environ.get("ECMWF_MARS_COMMAND", "mars")
    p = subprocess.Popen([mars_command, sys.argv[1]], cwd=os.path.dirname(__file__), shell=False, env=env)
    return p.wait()


if __name__ == "__main__":
    sys.exit(main())
