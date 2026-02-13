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

from ..common.metric_store import create_metric_store
from ..common.request_store import create_request_store
from ..common.staging import create_staging


class GarbageCollector:
    def __init__(self, config):

        gc_config = config.get("garbage-collector", {})

        s_interval = gc_config.get("interval", "60s")
        s_threshold = gc_config.get("threshold", "10G")
        s_age = gc_config.get("age", "24h")
        s_metric_age = gc_config.get("metric_age", "24h")
        self.interval = parse_time(s_interval).total_seconds()
        self.threshold = parse_bytes(s_threshold)
        self.age = parse_time(s_age)
        self.metric_age = parse_time(s_metric_age)

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

        self.request_store = create_request_store(config.get("request_store"), config.get("metric_store"))
        self.staging = create_staging(config.get("staging"))
        self.metric_store = create_metric_store(config.get("metric_store"))

    def run(self):
        while not time.sleep(self.interval):
            self.remove_old_requests()
            self.remove_old_metrics()
            self.remove_dangling_data()
            self.remove_by_size()

    def remove_old_requests(self):
        """Removes requests that are FAILED or PROCESSED after the configured time"""
        cutoff = datetime.now(timezone.utc) - self.age
        logging.info("Removing requests older than {}".format(cutoff))
        self.request_store.remove_old_requests(cutoff)

    def remove_old_metrics(self):
        """Removes metrics older than the configured time"""
        cutoff = datetime.now(timezone.utc) - self.metric_age
        self.metric_store.remove_old_metrics(cutoff)

    def remove_dangling_data(self):
        """As a failsafe, removes data which has no corresponding request."""
        logging.info("Removing dangling data with no corresponding request.")
        all_objects = self.staging.list()
        if not all_objects:
            return

        request_ids = set(self.request_store.get_request_ids())

        for data in all_objects:
            request_id = data.name.rsplit(".", 1)[0]

            if request_id not in request_ids:
                logging.info("Deleting {} because it has no matching request.".format(request_id))
                try:
                    self.staging.delete(data.name)  # TODO temporary fix for content-disposition error
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
                format_bytes(total_size),
                format_bytes(self.threshold),
                total_size / self.threshold * 100,
            )
        )

        if total_size < self.threshold:
            return

        # If we reached the total size limit, start deleting old data
        # Delete objects in ascending last_modified order (oldest first)

        all_objects_by_age = {
            d.name: {"size": d.size, "last_modified": d.last_modified}
            for d in sorted(all_objects, key=lambda x: x.last_modified)
        }
        removed_requests = []
        for name, v in all_objects_by_age.items():
            logging.info("Deleting {} because threshold reached and it is the oldest request.".format(name))
            try:
                self.staging.delete(name)
            except KeyError:
                logging.info("Data {} not found in staging.".format(name))

            if "." in name:
                name = name.split(".")[0]

            removed_requests.append(name)
            total_size -= v["size"]
            logging.info("Size of staging is {}/{}".format(format_bytes(total_size), format_bytes(self.threshold)))
            if total_size < self.threshold:
                break

        logging.info("Removing {} requests from request store.".format(len(removed_requests)))
        for request_id in removed_requests:
            self.request_store.remove_request(request_id)


##################################################################################

regex = re.compile(r"((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?")


def parse_time(time_str: str) -> timedelta:
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


def format_bytes(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_names = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB")
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return "{:.1f} {}".format(size_bytes, size_names[i])
