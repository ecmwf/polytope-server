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

import copy
import logging
import os
import tempfile
from subprocess import CalledProcessError

import requests
import yaml

from ..io.fifo import FIFO
from ..subprocess import Subprocess
from . import datasource
from .datasource import convert_to_mars_request


class MARSDataSource(datasource.DataSource):
    def __init__(self, config):
        assert config["type"] == "mars"
        self.config = config
        self.type = config.get("type")
        self.protocol = config.get("protocol", "dhs")

        # Default to direct client invocation; wrapper is no longer used.
        self.command = config.get("command", "/usr/local/bin/mars")
        self.tmp_dir = config.get("tmp_dir", "/tmp")

        self.override_mars_email = config.get("override_email")
        self.override_mars_apikey = config.get("override_apikey")

        self.subprocess = None
        self.fifo = None
        self.output_file = None
        self.use_file_io = config.get("use_file_io", False)

        self.mars_error_filter = config.get("mars_error_filter", "mars - EROR")

        # self.fdb_config = None
        self.fdb_config = config.get("fdb_config", {})
        if self.protocol == "remote":
            # need to set FDB5 config in a <path>/etc/fdb/config.yaml
            self.fdb_home = self.tmp_dir + "/fdb-home"
            # os.makedirs(self.fdb_home + "/etc/fdb/", exist_ok=True)
            # with open(self.fdb_home + "/etc/fdb/config.yaml", "w") as f:
            #     yaml.dump(self.fdb_config, f)

        # Write the mars config
        if "config" in config:
            self.mars_config = config.get("config", {})

            if self.protocol == "remote":
                self.mars_config[0]["home"] = self.fdb_home

            self.mars_home = self.tmp_dir + "/mars-home"
            os.makedirs(self.mars_home + "/etc/mars-client/", exist_ok=True)
            with open(self.mars_home + "/etc/mars-client/databases.yaml", "w") as f:
                yaml.dump(self.mars_config, f)
        else:
            self.mars_home = None
            self.mars_config = None

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError("Archiving not implemented for MARS data source")

    def retrieve(self, request):

        if self.use_file_io:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                self.output_file = tmp.name
            target = self.output_file
        else:
            # Open a FIFO for MARS output
            self.fifo = FIFO("MARS-FIFO-" + request.id)
            target = self.fifo.path

        # Parse the user request as YAML, and add the FIFO as target
        r = copy.deepcopy(request.coerced_request) or {}
        r["target"] = '"' + target + '"'

        # Make a temporary file for the request
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            self.request_file = tmp.name
            logging.info("Writing request to tempfile {}".format(self.request_file))
            tmp.write(convert_to_mars_request(r, "retrieve").encode())

        # Call MARS
        self.subprocess = Subprocess()
        self.subprocess.run(
            cmd=[self.command, self.request_file],
            cwd=os.path.dirname(__file__),
            env=self.make_env(request),
        )

        logging.info("MARS subprocess started with PID {}".format(self.subprocess.subprocess.pid))

        if self.use_file_io:
            while self.subprocess.running():
                self.subprocess.read_output(request, self.mars_error_filter)
            logging.info("MARS process finished.")
            return True

        # Poll until the FIFO has been opened by MARS, watch in case the spawned process dies before opening the FIFO
        try:
            while self.subprocess.running():
                # logging.debug("Checking if MARS process has opened FIFO.")  # this floods the logs
                if self.fifo.ready():
                    logging.info("FIFO is ready for reading.")
                    break

                self.subprocess.read_output(request, self.mars_error_filter)
            else:
                logging.info("Detected MARS process has exited before opening FIFO.")
                self.destroy(request)
                raise Exception("MARS process exited before returning data.")
        except Exception as e:
            logging.exception(f"Error while waiting for MARS process to open FIFO: {e}.")
            self.destroy(request)
            raise

        return True

    def result(self, request):

        if self.use_file_io:
            with open(self.output_file, "rb") as f:
                while True:
                    data = f.read(1024 * 1024)
                    if not data:
                        break
                    yield data

            try:
                self.subprocess.finalize(request, self.mars_error_filter)
            except CalledProcessError as e:
                logging.exception("MARS subprocess failed: {}".format(e))
                raise Exception("MARS retrieval failed unexpectedly with error code {}".format(e.returncode))
            return

        # The FIFO will get EOF if MARS exits unexpectedly, so we will break out of this loop automatically
        for x in self.fifo.data():
            # logging.debug("Yielding data from FIFO.")  # this floods the logs
            self.subprocess.read_output(request, self.mars_error_filter)
            yield x

        logging.info("FIFO reached EOF.")

        try:
            self.subprocess.finalize(request, self.mars_error_filter)
        except CalledProcessError as e:
            logging.exception("MARS subprocess failed: {}".format(e))
            raise Exception("MARS retrieval failed unexpectedly with error code {}".format(e.returncode))

    def destroy(self, request):
        try:
            self.subprocess.finalize(request, self.mars_error_filter)  # Will raise if non-zero return
        except Exception as e:
            logging.info("MARS subprocess failed: {}".format(e))
            pass
        try:
            os.unlink(self.request_file)
        except Exception:
            pass
        try:
            if self.use_file_io:
                if self.output_file:
                    os.unlink(self.output_file)
            else:
                self.fifo.delete()
        except Exception:
            pass

    def mime_type(self) -> str:
        return "application/x-grib"

    #######################################################

    def _build_dhs_env(self):
        """Build DHS callback environment from the pod's Kubernetes service."""

        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        port_file = "/persistent/last_mars_port"

        with open(token_path, "r") as file:
            token = file.read().strip()

        headers = {"Authorization": "Bearer " + token}

        node_name = os.environ["K8S_NODE_NAME"]
        pod_name = os.environ["K8S_POD_NAME"]
        namespace = os.environ["K8S_NAMESPACE"]

        service_url = (
            f"https://{os.environ['KUBERNETES_SERVICE_HOST']}:"
            f"{os.environ['KUBERNETES_PORT_443_TCP_PORT']}/api/v1/namespaces/"
            f"{namespace}/services/{pod_name}"
        )
        response = requests.get(service_url, headers=headers, verify=ca_path)
        response.raise_for_status()
        service = response.json()["spec"]

        try:
            with open(port_file, "rt") as f:
                last_port_id = int(f.read())
        except FileNotFoundError:
            last_port_id = 0

        ports = service.get("ports", [])
        if not ports:
            raise RuntimeError("No ports defined for DHS service")

        port_id = (last_port_id + 1) % len(ports)

        with open(port_file, "w+") as f:
            f.write(str(port_id))

        node_port = ports[port_id]["nodePort"]
        local_port = ports[port_id]["port"]

        logging.info("DHS callback configured on %s:%s (local %s)", node_name, node_port, local_port)

        return {
            "MARS_ENVIRON_ORIGIN": "polytope",
            "MARS_DHS_CALLBACK_HOST": node_name,
            "MARS_DHS_CALLBACK_PORT": str(node_port),
            "MARS_DHS_LOCALPORT": str(local_port),
            "MARS_DHS_LOCALHOST": pod_name,
        }

    def make_env(self, request):
        """Make the environment for the MARS subprocess, primarily for setting credentials"""
        try:
            if self.override_mars_email:
                logging.info("Overriding MARS_USER_EMAIL with {}".format(self.override_mars_email))
                mars_user = self.override_mars_email
            else:
                mars_user = request.user.attributes.get("ecmwf-email", "no-email")

            if self.override_mars_apikey:
                logging.info("Overriding MARS_USER_TOKEN with {}".format(self.override_mars_apikey))
                mars_token = self.override_mars_apikey
            else:
                mars_token = request.user.attributes.get("ecmwf-apikey", "no-api-key")

            env = {
                **os.environ,
                "MARS_USER_EMAIL": mars_user,
                "MARS_USER_TOKEN": mars_token,
                "ECMWF_MARS_COMMAND": self.command,
                "FDB5_CONFIG": yaml.dump(self.fdb_config),
            }

            if self.mars_config is not None:
                env["MARS_HOME"] = self.mars_home

            if self.protocol == "dhs":
                env.update(self._build_dhs_env())

            logging.info("Accessing MARS on behalf of user {} with token {}".format(mars_user, mars_token))

        except Exception as e:
            logging.error("MARS request aborted because user does not have associated ECMWF credentials")
            raise e

        return env
