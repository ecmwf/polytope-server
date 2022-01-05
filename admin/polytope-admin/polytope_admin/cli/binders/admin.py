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

import click
from docstring_parser import parse

from ...api.Client import Client
from . import helpers

# create
doc = parse(Client.create_user.__doc__)


@click.command(
    short_help=doc.short_description,
    help=(doc.params[0].arg_name.upper() + ": " + doc.params[0].description + "\n\n" + doc.long_description),
)
@click.argument("user_name", type=str)
@click.option(
    "pass_word",
    "--new-password",
    required=True,
    prompt="New user's password",
    hide_input=True,
    confirmation_prompt=True,
    help=doc.params[1].description,
)
@click.option("affiliation", "--affiliation", required=True, prompt=True, help=doc.params[2].description)
@click.option("role", "--role", default="guest", show_default=True, help=doc.params[3].description)
@click.option("soft_limit", "--soft-limit", default=1, show_default=True, type=int, help=doc.params[4].description)
@click.option("hard_limit", "--hard-limit", default=10, show_default=True, type=int, help=doc.params[5].description)
@helpers.user_configurable
def create(**kwargs):
    session_args, other_args = helpers.filter_session_args(**kwargs)
    other_args["username"] = other_args["user_name"]
    del other_args["user_name"]
    other_args["password"] = other_args["pass_word"]
    del other_args["pass_word"]
    Client(**session_args).create_user(**other_args)


# delete
doc = parse(Client.delete_user.__doc__)


@click.command(
    short_help=doc.short_description,
    help=(doc.params[0].arg_name.upper() + ": " + doc.params[0].description + "\n\n" + doc.long_description),
)
@click.argument("user_name", type=str)
@helpers.user_configurable
def delete(**kwargs):
    session_args, other_args = helpers.filter_session_args(**kwargs)
    other_args["username"] = other_args["user_name"]
    del other_args["user_name"]
    Client(**session_args).delete_user(**other_args)


# ping
doc = parse(Client.ping.__doc__)


@click.command(short_help=doc.short_description, help=doc.long_description)
@helpers.user_configurable
def ping(**kwargs):
    session_args, other_args = helpers.filter_session_args(**kwargs)
    Client(**session_args).ping(**other_args)
