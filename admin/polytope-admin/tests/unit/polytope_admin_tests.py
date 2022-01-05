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

import re
import signal

from click.testing import CliRunner
from polytope_admin.api import Client
from polytope_admin.cli.cli import cli

# CONFIGURABLE PARAMETERS
#
# If all tests go through, the status of the Polytope server is left unmodified
# (all users and  created by the tests are finally removed).
# However if one of the tests fails, the cleanup may not take place and, when
# re-running the tests, they won't pass because the users and  may
# already exist on the server, and the received response messages will be
# different than if the server was clean. In consequence, the tests won't pass.
# In order to fix this and make the tests pass again, there are a few options:
#  - to redeploy the server, so that the databases are wiped
#  - to clean the server manually (e.g. using the polytope client to remove
#    test users and )
#  - to modify the parameters of the test below this paragraph, so that
#    different users and  are created each time
#
# User names used in the tests
user_prefix = ""
test_users = [user_prefix + "_test_user", user_prefix + "_test_user_2"]
#
###############################################################################

# HELPER FUNCTIONS


# helper for tests that require a timeout
# see https://stackoverflow.com/questions/492519/timeout-on-a-function-call
def timeout(time, fun, kwargs):
    class TimeOverException(Exception):
        pass

    def handler(signum, frame):
        raise TimeOverException("Exceeded timeout")

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(time)
    res = {"timed_out": False, "output": None}
    try:
        res["output"] = fun(**kwargs)
    except TimeOverException:
        res["timed_out"] = True
    else:
        signal.alarm(0)
    return res


# TESTS

runner = CliRunner()


def test_set_server():
    # test the set server command effectively sets the new configuration in the config file
    test_address = "192.168.1.1"
    test_port = "6789"
    result = runner.invoke(cli, ["set", "config", "address", test_address])
    print(result.stdout)
    result = runner.invoke(cli, ["set", "config", "port", test_port])
    print(result.stdout)

    assert Client().config.get_url("api_root") == "https://" + test_address + ":" + test_port + "/api/v1"


def test_create_user(internal_address, internal_port, admin_username, admin_password):
    result = runner.invoke(cli, ["unset", "config", "all"])
    print(result.stdout)
    result = runner.invoke(cli, ["set", "config", "address", internal_address])
    print(result.stdout)
    result = runner.invoke(cli, ["set", "config", "port", internal_port])
    print(result.stdout)

    # test authenticating a user and getting a key
    # result = runner.invoke(cli, ['login', admin_username, '--login-password', admin_password])
    # print(result.stdout)
    # assert result.exit_code == 0
    # assert re.match(r'[\s\S]*User key received successfully[\s\S]*', result.output)

    # test users can be created
    result = runner.invoke(cli, ["set", "config", "username", admin_username])
    print(result.stdout)
    result = runner.invoke(cli, ["set", "config", "password", admin_password])
    print(result.stdout)
    for username in test_users:
        result = runner.invoke(
            cli,
            [
                "create",
                "user",
                username,
                "--new-password",
                "hola",
                "--affiliation",
                "hola",
                "--role",
                "admin",
                "--soft-limit",
                "2",
                "--hard-limit",
                "5",
            ],
        )
        print(result.stdout)
        assert result.exit_code == 0
        assert re.match(r"[\s\S]*User created successfully[\s\S]*", result.output)

    # test trying to create an existing user fails
    result = runner.invoke(
        cli,
        [
            "create",
            "user",
            test_users[0],
            "--new-password",
            "hola",
            "--affiliation",
            "hola",
            "--role",
            "admin",
            "--soft-limit",
            "2",
            "--hard-limit",
            "5",
        ],
    )
    print(str(result.exception))
    assert result.exit_code == 1
    assert re.match(r"[\s\S]*CLIENT ERROR[\s\S]*already registered[\s\S]*", str(result.exception))
    result = runner.invoke(cli, ["unset", "config", "username"])
    print(result.stdout)
    result = runner.invoke(cli, ["unset", "config", "password"])
    print(result.stdout)

    # test creating a user via external URL fails

    # additional user creation tests


def test_authenticate_user(address, port):
    result = runner.invoke(cli, ["set", "config", "address", address])
    print(result.stdout)
    result = runner.invoke(cli, ["set", "config", "port", port])
    print(result.stdout)

    # test authenticating a non-existing Polytope user fails
    result = runner.invoke(cli, ["login", "_test_user_not_exists", "--login-password", "hola"])
    print(str(result.exception))
    assert result.exit_code == 1
    assert re.match(r"[\s\S]*Incorrect login details[\s\S]*", str(result.exception))

    # repeat for ecmwf

    # repeat for existing Basic user and expect 40X?

    # test authenticating a user with wrong credentials fails
    result = runner.invoke(cli, ["login", test_users[0], "--login-password", "hello", "--key-type", "bearer"])
    print(str(result.exception))
    assert result.exit_code == 1
    assert re.match(r"[\s\S]*CLIENT ERROR[\s\S]*", str(result.exception))

    # repeat for ecmwf

    # repeat for Basic user

    # test authenticating a user and getting a key
    result = runner.invoke(cli, ["login", test_users[0], "--login-password", "hola", "--key-type", "bearer"])
    print(result.stdout)
    assert result.exit_code == 0
    assert re.match(r"[\s\S]*User key received successfully[\s\S]*", result.output)

    result = runner.invoke(cli, ["unset", "config", "user_key"])
    print(result.stdout)

    # repeat for ecmwf. check that ECMWF_USERNAME and ECMWF_PASSWORD are set in the environment
    # if not, skip?
    # check ecmwf_ configs are populated

    # repeat for Basic. expect 401

    # run one authenticated command with each auth type


def test_delete_user(internal_address, internal_port, admin_username, admin_password):
    result = runner.invoke(cli, ["set", "config", "address", internal_address])
    print(result.stdout)
    result = runner.invoke(cli, ["set", "config", "port", internal_port])
    print(result.stdout)

    # test users can be deleted
    result = runner.invoke(cli, ["set", "config", "username", admin_username])
    print(result.stdout)
    result = runner.invoke(cli, ["set", "config", "password", admin_password])
    print(result.stdout)
    for username in test_users:
        result = runner.invoke(cli, ["delete", "user", username])
        print(result.stdout)
        assert result.exit_code == 0
        assert re.match(r"[\s\S]*User deleted successfully[\s\S]*", result.output)

    # test trying to delete a non-existing user fails
    result = runner.invoke(cli, ["delete", "user", test_users[0]])
    print(str(result.exception))
    assert result.exit_code == 1
    assert re.match(r"[\s\S]*CLIENT ERROR[\s\S]*", str(result.exception))

    result = runner.invoke(cli, ["unset", "config", "username"])
    print(result.stdout)
    result = runner.invoke(cli, ["unset", "config", "password"])
    print(result.stdout)
