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

import docker


def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    assert len(sys.argv) == 2

    c = docker.from_env()
    # c = docker.DockerClient(base_url = ('tcp://' +
    #   os.environ['POLYTOPE_DOCKER_URL']))

    container_name = os.environ["HOSTNAME"]
    containers = c.containers.list()
    cids = list(map(lambda x: x.short_id, containers))
    container = None
    for cpos in range(len(cids)):
        if container_name.startswith(cids[cpos]):
            container = containers.pop(cpos)
            break
    if not container:
        raise Exception("Container not found")

    container_port = str(os.environ.get("POLYTOPE_WORKER_MARS_LOCALPORT"))
    external_port = container.ports[container_port + "/tcp"][0]["HostPort"]
    swarm_node = container.labels["com.docker.swarm.node.id"]
    nodes = c.nodes.list()
    nids = list(map(lambda x: x.attrs["ID"], nodes))
    node = None
    for npos in range(len(nids)):
        if swarm_node == nids[npos]:
            node = nodes.pop(npos)
            break
    if not node:
        raise Exception("Node not found")

    node_name = node.attrs["Description"]["Hostname"]

    mars_command = os.environ.get("ECMWF_MARS_COMMAND", "mars")

    # Set the MARS client environment variables

    env = {
        **os.environ,
        "MARS_ENVIRON_ORIGIN": "polytope",
        "MARS_DHS_CALLBACK_HOST": node_name,
        "MARS_DHS_CALLBACK_PORT": external_port,
        "MARS_DHS_LOCALPORT": container_port,
        "MARS_DHS_LOCALHOST": node_name,
    }

    # env = os.environ.copy()

    # def demote(user_uid, user_gid):
    #    def result():
    #        report_ids('starting demotion')
    #        os.setgid(user_gid)
    #        os.setuid(user_uid)
    #        report_ids('finished demotion')
    #    return result

    # def report_ids(msg):
    #    print('uid, gid = %d, %d; %s' % (os.getuid(), os.getgid(), msg))

    p = subprocess.Popen([mars_command, sys.argv[1]], cwd=os.path.dirname(__file__), shell=False, env=env)
    return p.wait()


if __name__ == "__main__":
    sys.exit(main())
