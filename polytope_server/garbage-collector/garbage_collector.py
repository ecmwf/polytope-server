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
import re
import time
from datetime import datetime, timedelta, timezone

from ..common.request import Status
from ..common.request_store import create_request_store
from ..common.staging import create_staging


class GarbageCollector:
    def __init__(self, config):

        gc_config = config.get("garbage-collector", {})

        s_interval = gc_config.get("interval", "60s")
        s_threshold = gc_config.get("threshold", "10G")
        s_age = gc_config.get("age", "24h")
        self.interval = parse_time(s_interval).total_seconds()
        self.threshold = parse_bytes(s_threshold)
        self.age = parse_time(s_age)

        logging.info(
            "Garbage collector initialized:\n Interval: {} ({} secs) \n \
             Threshold: {} ({} bytes)\n Age Limit: {} ({})".format(
                s_interval,
                self.interval,
                s_threshold,
                self.threshold,
                s_age,
                self.age,
            )
        )

        self.request_store = create_request_store(config.get("request_store"))
        self.staging = create_staging(config.get("staging"))

    def run(self):
        while not time.sleep(self.interval):
            self.remove_old_requests()
            self.remove_dangling_data()
            self.remove_by_size()

    def remove_old_requests(self):
        """Removes requests that are FAILED or PROCESSED after the configured time"""
        now = datetime.now(timezone.utc)
        cutoff = now - self.age

        requests = self.request_store.get_requests(status=Status.FAILED) + self.request_store.get_requests(
            status=Status.PROCESSED
        )

        for r in requests:
            if datetime.fromtimestamp(r.last_modified, tz=timezone.utc) < cutoff:
                logging.info("Deleting {} because it is too old.".format(r.id))
                try:
                    self.staging.delete(r.id)
                except KeyError:
                    pass
                self.request_store.remove_request(r.id)

    def remove_dangling_data(self):
        """As a failsafe, removes data which has no corresponding request."""
        all_objects = self.staging.list()
        for data in all_objects:
            request = self.request_store.get_request(id=data.name)
            if request is None:
                logging.info("Deleting {} because it has no matching request.".format(data.name))
                try:
                    self.staging.delete(data.name)
                except KeyError:
                    # TODO: why does this happen?
                    logging.info("Data {} not found in staging.".format(data.name))

    def remove_by_size(self):
        """Cleans data according to size limits of the staging, removing older requests first."""

        all_objects = self.staging.list()

        total_size = 0
        for data in all_objects:
            total_size += data.size

        logging.info(
            "Found {} items in staging -- {}/{} bytes -- {:3.1f}%".format(
                len(all_objects),
                total_size,
                self.threshold,
                total_size / self.threshold * 100,
            )
        )

        all_objects_by_age = {}

        for data in all_objects:
            request = self.request_store.get_request(id=data.name)
            all_objects_by_age[data.name] = {
                "size": data.size,
                "last_modified": request.last_modified,
            }

        if total_size < self.threshold:
            return

        # If we reached the total size limit, start deleting old data
        # Delete objects in ascending last_modified order (oldest first)
        for name, v in sorted(all_objects_by_age.items(), key=lambda x: x[1]["last_modified"]):
            logging.info("Deleting {} because threshold reached and it is the oldest request.".format(name))
            try:
                self.staging.delete(name)
            except KeyError:
                logging.info("Data {} not found in staging.".format(name))
            self.request_store.remove_request(name)
            total_size -= v["size"]
            logging.info("Size of staging is {}/{}".format(total_size, self.threshold))
            if total_size < self.threshold:
                return


##################################################################################

regex = re.compile(r"((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?")


def parse_time(time_str):
    parts = regex.match(time_str)
    if not parts:
        return
    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)


def parse_bytes(size_str):
    size = int(float(size_str[:-1]))
    suffix = size_str[-1].upper()

    if suffix == "K":
        return size * 1024
    elif suffix == "M":
        return size * 1024**2
    elif suffix == "G":
        return size * 1024**3
    elif suffix == "T":
        return size * 1024**4

    return False
