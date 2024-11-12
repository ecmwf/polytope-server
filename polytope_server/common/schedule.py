#!/usr/bin/env python3
from __future__ import print_function

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Optional, Tuple


class ScheduleReader:
    def __init__(self, schedule_file: str) -> None:
        self.tree = ET.parse(schedule_file)
        self.products: List[ET.Element] = self.tree.findall("product") + self.tree.findall("dissemination_only/product")

    def get_release_time_and_delta_day(
        self, cclass: str, stream: str, domain: str, time: str, step: str, diss_type: str
    ) -> Tuple[Optional[str], Optional[int]]:
        """
        Retrieves dissemination time from the schedule for respective stream etc.

        Parameters
        ----------
        cclass : string
            forecast class, e.g., od | ai | ..
        stream : string
            data stream, e.g., oper | scda | ..
        domain : string
            data domain, e.g., g | m | ..
        time : string
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
            if itree.findtext("time") != time:
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
                    "release time: {} with delta_day: {} found for stream: {}, type: {}, "
                    "domain: {}, time: {}, step: {}".format(
                        release_time, delta_day, stream, diss_type, domain, time, step
                    )
                )
                return release_time, delta_day

        logging.warning(
            "No release time found for stream: {}, type: {}, domain: {}, time: {}, step: {}".format(
                stream, diss_type, domain, time, step
            )
        )
        return None, None

    def is_released(
        self, date_in: datetime, cclass: str, stream: str, domain: str, time: str, step: str, diss_type: str
    ) -> bool:
        """
        Checks if the data is released or not

        Parameters
        ----------
        date_in : datetime
            production date of the data
        cclass : string
            forecast class, e.g., od | ai | ..
        stream : string
            data stream, e.g., oper | scda | ..
        domain : string
            data domain, e.g., g | m | ..
        time : string
            production time of the data, i.e., 00:00 | 06:00 | 12:00 | 18:00
        step : string
            data time step, e.g., 0 | 1 | .. | 360 | ..
        diss_type : string
            data type, e.g., fc | an | ..

        Returns
        -------
        bool
            True if data is released, False otherwise
        """
        release_time, delta_day = self.get_release_time_and_delta_day(cclass, stream, domain, time, step, diss_type)
        if release_time is None:
            return False

        release_time_dt = datetime.strptime(release_time, "%H:%M:%S")
        release_date = date_in + timedelta(days=delta_day)
        release_date = release_date.replace(
            hour=release_time_dt.hour, minute=release_time_dt.minute, second=release_time_dt.second
        )
        return datetime.now() > release_date


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
