#!/usr/bin/env python3
from __future__ import print_function

import itertools
import logging
import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from polytope_feature.utility.exceptions import PolytopeError

schedule_file_path = os.path.join("/etc/polytope_schedule/schedule.xml")


class ScheduleReader:
    def __init__(self, schedule_file: str) -> None:
        self.products: List[Dict[str, Any]] = self.load_products(schedule_file)

    def load_products(self, schedule_file: str) -> List[Dict[str, Any]]:
        tree = ET.parse(schedule_file)
        products = tree.findall("product")
        mars_only = tree.findall("mars_only")
        if mars_only is not None:
            for mars in mars_only:
                products.extend(mars.findall("product"))
        product_dicts = []
        for product in products:
            product_dict = {child.tag: child.text for child in product}
            product_dicts.append(product_dict)
        return product_dicts

    def check_released(
        self, date_in: str, cclass: str, stream: str, domain: str, time_in: str, step: str, ttype: str
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
        ttype : string
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
        date_in = datetime.strftime(max(map(parse_mars_date, split_mars_param(date_in))), "%Y-%m-%d")
        time_in = max(map(parse_mars_time, split_mars_param(time_in))).strftime("%H:%M")
        step = str(max(map(int, split_mars_param(step)))).zfill(4)

        cclass = split_mars_param(cclass)
        stream = split_mars_param(stream)
        domain = split_mars_param(domain)
        ttype = split_mars_param(ttype)

        for c, s, dom, diss in itertools.product(cclass, stream, domain, ttype):
            release_time, delta_day = self.get_release_time_and_delta_day(c, s, dom, time_in, step, diss)
            if release_time is None:
                raise PolytopeError(
                    f"No release time found for date: {date_in}, class: {c}, stream: {s}, "
                    f"domain: {dom}, time: {time_in}, step: {step}, type {diss}"
                )

            release_time_dt = datetime.strptime(release_time, "%H:%M:%S")
            release_date = datetime.fromisoformat(date_in) + timedelta(days=delta_day)
            release_date = release_date.replace(
                hour=release_time_dt.hour, minute=release_time_dt.minute, second=release_time_dt.second
            )
            if datetime.now() < release_date:
                raise PolytopeError(
                    f"Data not released yet. Release time is {release_date}."
                    # f"Data not yet released for date: {date_in}, class: {c}, stream: {s}, "
                    # f"domain: {dom}, time: {time_in}, step: {step}, type {diss}. "
                    # f"Release time is {release_date}."
                )

    def get_release_time_and_delta_day(
        self, cclass: str, stream: str, domain: str, time_in: str, step: str, ttype: str
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
        ttype : string
            data type, e.g., fc | an | ..

        Returns
        -------
        release_time: string
            time of release (hh:mm:ss)
        delta_day: int
            number of days to add to the production date
        """

        def matches_criteria(product: Dict[str, Any]) -> bool:
            if product.get("class") != cclass:
                return False
            if stream.lower() not in product["stream"].lower():
                return False
            if time_in != product.get("time"):
                return False
            prod_domain = find_tag(product, "domain")
            if prod_domain:
                if domain.lower() != find_tag(product, "domain"):
                    return False
            prod_type = find_tag(product, "type")
            if prod_type:
                if ttype.lower() not in find_tag(product, "type"):
                    return False
            if cclass == "ai":
                return True

            prod_step = find_tag(product, "step")
            if prod_step:
                istep = int(prod_step) if prod_step is not None else prod_step
                if istep != int(step):
                    return False
            return True

        for product in self.products:
            if matches_criteria(product):
                release_time = product.get("release_time")
                delta_day = int(product.get("release_delta_day", 0))
                logging.info(
                    "release time: {} with delta_day: {} found for class: {}, stream: {}, type: {}, "
                    "domain: {}, time: {}, step: {}".format(
                        release_time, delta_day, cclass, stream, ttype, domain, time_in, step
                    )
                )
                return release_time, delta_day

        logging.warning(
            "No release time found for class{}, stream: {}, type: {}, domain: {}, time: {}, step: {}".format(
                cclass, stream, ttype, domain, time_in, step
            )
        )
        return None, None


def parse_mars_date(mars_date: str) -> date:
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
            return date.today() - timedelta(days=-delta)
        except ValueError:
            raise PolytopeError(f"Invalid date format: {mars_date}")


def parse_mars_time(mars_time: str) -> time:
    """
    Parse a MARS time string into a time object.
    Valid formats are: %H, %H%M, %H:%M

    Parameters
    ----------
    mars_time : str
        The time string to parse.

    Returns
    -------
    time
        The parsed time object.
    """
    time_formats = ["%H", "%H%M", "%H:%M"]
    for time_format in time_formats:
        try:
            return datetime.strptime(mars_time, time_format).time()
        except ValueError:
            continue
    raise ValueError(f"Invalid time format: {mars_time}")


def split_mars_param(param: str) -> List[str]:
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


def find_tag(product: Dict[str, Any], keyword: str) -> Optional[str]:
    """
    Utility function to find a tag in the product dictionary,
    checking for both 'diss_{keyword}' and '{keyword}' tags. Used with "step" and "domain" tags.

    Parameters
    ----------
    product : Dict[str, Any]
        The product dictionary to search within.
    keyword : str
        The tag to search for.

    Returns
    -------
    Optional[str]
        The text of the tag if found, otherwise None.
    """
    tag = product.get(keyword)
    if tag is None:
        tag = product.get(f"diss_{keyword}")
    return tag


if os.environ["SCHEDULE_ENABLED"].lower() == "true":
    if os.path.exists(schedule_file_path):
        SCHEDULE_READER = ScheduleReader(schedule_file_path)
    else:
        raise FileNotFoundError(f"Schedule is enabled, but schedule file not found at {schedule_file_path}")
