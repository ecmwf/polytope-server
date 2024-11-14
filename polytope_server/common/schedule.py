#!/usr/bin/env python3
from __future__ import print_function

import itertools
import logging
import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timedelta
from typing import List, Optional, Tuple

from polytope_feature.utility.exceptions import PolytopeError

script_dir = os.path.dirname(os.path.abspath(__file__))
schedule_file_path = os.path.join(script_dir, "schedule.xml")


class ScheduleReader:
    def __init__(self, schedule_file: str) -> None:
        self.tree = ET.parse(schedule_file)
        self.products: List[ET.Element] = self.tree.findall("product") + self.tree.findall("dissemination_only/product")

    def check_released(
        self, date_in: str, cclass: str, stream: str, domain: str, time_in: str, step: str, diss_type: str
    ) -> None:
        """
        Checks if the data is released or not. Accepts arrays and ranges.

        Parameters
        ----------
        date_in : str
            production date (or range) of the data,
            see https://confluence.ecmwf.int/pages/viewpage.action?pageId=118817289
        cclass : string
            forecast class, e.g., od | ai | ..
        stream : string
            data stream, e.g., oper | scda | ..
        domain : string
            data domain, e.g., g | m | ..
        time_in : string
            production time of the data, i.e., 00:00 | 06:00 | 12:00 | 18:00
        step : string
            data time step, e.g., 0 | 1 | .. | 360 | ..
        diss_type : string
            data type, e.g., fc | an | ..

        Returns
        -------
        None

        Raises
        ------
        PolytopeError
            If the data is not released yet.
        """
        # Get only latest production date and time, last step
        date_in = max(map(parse_mars_date, split_mars_param(date_in)))
        time_in = max(map(lambda x: datetime.strptime(x, "%H:%M").time(), split_mars_param(time_in)))
        step = max(map(int, split_mars_param(step)))

        cclass = split_mars_param(cclass)
        stream = split_mars_param(stream)
        domain = split_mars_param(domain)
        diss_type = split_mars_param(diss_type)

        for c, s, dom, diss in itertools.product(cclass, stream, domain, diss_type):
            release_time, delta_day = self.get_release_time_and_delta_day(c, s, dom, time_in, step, diss)
            if release_time is None:
                raise PolytopeError(
                    f"No release time found for date: {date_in}, class: {c}, stream: {s}, "
                    f"domain: {dom}, time: {time_in}, step: {step}, type {diss}"
                )

            release_time_dt = datetime.strptime(release_time, "%H:%M:%S")
            release_date = date_in + timedelta(days=delta_day)
            release_date = release_date.replace(
                hour=release_time_dt.hour, minute=release_time_dt.minute, second=release_time_dt.second
            )
            if datetime.now() < release_date:
                raise PolytopeError(
                    f"Data not yet released for date: {date_in}, class: {c}, stream: {s}, "
                    f"domain: {dom}, time: {time_in}, step: {step}, type {diss}"
                )

    def get_release_time_and_delta_day(
        self, cclass: str, stream: str, domain: str, time_in: str, step: str, diss_type: str
    ) -> Tuple[Optional[str], Optional[int]]:
        """
        Retrieves dissemination time from the schedule for respective stream etc.
        DOES NOT ACCEPT ARRAYS OR RANGES.

        Adapted from ecmwf/pgen/src/scripts/pgen-crack-schedule

        Parameters
        ----------
        cclass : string
            forecast class, e.g., od | ai | ..
        stream : string
            data stream, e.g., oper | scda | ..
        domain : string
            data domain, e.g., g | m | ..
        time_in : string
            production time of the data, i.e., 00:00 | 06:00 | 12:00 | 18:00
        step : string
            data time step, e.g., 0 | 1 | .. | 360 | ..
        diss_type : string
            data type, e.g., fc | an | ..

        Returns
        -------
        release_time: string
            time of release (hh:mm:ss)
        delta_day: int
            number of days to add to the production date
        """

        def matches_criteria(itree: ET.Element) -> bool:
            if itree.findtext("class") != cclass:
                return False
            if stream.lower() not in itree.findtext("stream"):
                return False
            if domain.lower() != find_tag(itree, "domain"):
                return False
            if itree.findtext("time") != time_in:
                return False
            if itree.findtext("diss_type") != diss_type.lower():
                return False
            if cclass != "ai":
                tmp_step = find_tag(itree, "step")
                istep = int(tmp_step) if tmp_step is not None else tmp_step
                if istep != int(step):
                    return False
            return True

        for itree in self.products:
            if matches_criteria(itree):
                release_time = itree.findtext("release_time")
                delta_day = int(itree.findtext("release_delta_day"))
                logging.info(
                    "release time: {} with delta_day: {} found for class: {}, stream: {}, type: {}, "
                    "domain: {}, time: {}, step: {}".format(
                        release_time, delta_day, cclass, stream, diss_type, domain, time_in, step
                    )
                )
                return release_time, delta_day

        logging.warning(
            "No release time found for class{}, stream: {}, type: {}, domain: {}, time: {}, step: {}".format(
                cclass, stream, diss_type, domain, time, step
            )
        )
        return None, None


def parse_mars_date(mars_date: str) -> datetime:
    """
    Parse a MARS date string into a datetime object.
    Valid formats are:

    Absolute as YYYY-MM-DD, YYYYMMDD. The day of the year can also be used: YYYY-DDD
    Relative as -n ; n is the number of days before today (i.e., -1 = yesterday )
    Name of month (e.g. January for Climatology data)
    Operational monthly means are retrieved by setting day (DD) to 00.
    See https://confluence.ecmwf.int/pages/viewpage.action?pageId=118817289 for more information.

    Parameters
    ----------
    date : str
        The date string to parse.

    Returns
    -------
    date
        The parsed date object.
    """
    try:
        return date.fromisoformat(mars_date)
    except ValueError:
        try:
            delta = int(mars_date)
            if delta > 0:
                raise PolytopeError(f"Invalid date format: {mars_date}")
            return date.today() - timedelta(-int(mars_date))
        except ValueError:
            raise PolytopeError(f"Invalid date format: {mars_date}")


def split_mars_param(
    param: str,
) -> List[str]:
    """
    Parse a MARS parameter string into an array if it is
    one or get the last element if it's a range.

    Parameters
    ----------
    param : str
        The parameter string to parse.

    Returns
    -------
    List[str]
        The split parameter string
    """
    parts = param.split("/")
    if "by" in parts:
        return parts[-3]
    if "to" in parts:
        return parts[-1]
    return parts


def find_tag(tree_elem: ET.Element, keyword: str) -> Optional[str]:
    """
    Utility function to find a tag in the tree element,
    checking for both 'diss_{keyword}' and '{keyword}' tags. Used with "step" and "domain" tags.

    Parameters
    ----------
    tree_elem : ET.Element
        The XML element to search within.
    keyword : str
        The tag to search for.

    Returns
    -------
    Optional[str]
        The text of the tag if found, otherwise None.
    """
    tag = tree_elem.findtext(f"diss_{keyword}")
    if tag is None:
        tag = tree_elem.findtext(keyword)
    if tag is None:
        raise IOError(f"Couldn't find forecast {keyword} as either 'diss_{keyword}' or '{keyword}'")
    return tag


if os.path.exists(schedule_file_path):
    SCHEDULE_READER = ScheduleReader(schedule_file_path)
else:
    logging.warning("schedule.xml file not found. No schedule rules will be applied.")
    SCHEDULE_READER = None
