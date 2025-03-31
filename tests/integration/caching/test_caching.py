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

import concurrent.futures
import logging
import time
from abc import ABC

import pytest

from polytope_server.common.caching import cache
from polytope_server.common.config import ConfigParser

# We have to use a global to make sure its ignored by the caching
executions = 0


class base(ABC):
    @cache()  # should not be inherted
    def do_stuff_base(self, number):
        global executions
        executions += 1
        return "do_stuff_base"


class child(base):
    def do_stuff_base(self, number):
        """Mimics overriding a base class, demonstrating that it does not
        inherit the caching rules
        """
        global executions
        executions += 1
        return "do_stuff_overriden"

    @cache()
    def do_stuff(self, number):
        global executions
        executions += 1
        return number + 1

    @cache()
    def do_stuff_cancel(self, number):
        global executions
        cache.cancel()
        executions += 1
        return number - 1

    @cache()
    def do_stuff_raises(self, number):
        global executions
        executions += 1
        raise NotImplementedError()
        return number


class WithCacheIdOverride(object):
    def __init__(self, value):
        self.value = value

    def cache_id(self):
        return self.value

    @cache()
    def do_stuff(self, number):
        global executions
        executions += 1
        return number


@cache()
def do_stuff(x=None):
    global executions
    executions += 1
    return 1


@cache()
def do_stuff_no_return(x=None):
    global executions
    executions += 1
    return None


@cache()
def do_stuff_parallel(x=None):
    global executions
    executions += 1
    while executions < 2:
        time.sleep(0.1)
    return 1


@pytest.mark.basic
class Test:
    def setup_method(self, method):
        logging.getLogger().setLevel("DEBUG")
        config = ConfigParser().read()
        cache.init(config.get("caching", {}))
        global executions
        executions = 0
        cache.wipe()

    def test_cache(self):
        # global executions
        c = child()
        c.do_stuff(1)
        c.do_stuff(1)
        assert executions == 1
        c.do_stuff(2)
        assert executions == 2
        c.do_stuff(1)
        assert executions == 2

    def test_cache_two_objects(self):
        # global executions
        c1 = child()
        c2 = child()
        c1.do_stuff(1)
        c2.do_stuff(1)
        assert executions == 1
        c2.diff = True  # changes the pickle of c2
        c2.do_stuff(1)
        assert executions == 2
        c1.diff = True  # now they are the same
        c1.do_stuff(1)
        assert executions == 2

    def test_cache_no_cache_inherited(self):
        # global executions
        b = base()
        b.do_stuff_base(1)
        b.do_stuff_base(1)
        assert executions == 1
        c = child()
        child()
        c.do_stuff_base(1)
        assert executions == 2
        c.do_stuff_base(1)  # not cached, decorator doesn't apply
        assert executions == 3

    def test_cache_cancelled(self):
        # global executions
        c = child()
        c.do_stuff_cancel(1)
        assert executions == 1
        c.do_stuff_cancel(1)  # not cached, function cancelled it
        assert executions == 2

    def test_cache_raised(self):
        # global executions
        c = child()

        with pytest.raises(NotImplementedError):
            c.do_stuff_raises(1)
        assert executions == 1

        with pytest.raises(NotImplementedError):
            c.do_stuff_raises(1)  # not cached, first call raised exception
        assert executions == 2

    def test_cache_function(self):
        # global executions
        do_stuff()
        assert executions == 1
        do_stuff()
        assert executions == 1
        do_stuff("hello")
        assert executions == 2
        do_stuff({"a": {"b": "c"}})
        assert executions == 3
        do_stuff({"a": {"b": "d"}})
        assert executions == 4
        do_stuff({"a": {"b": "c"}})
        assert executions == 4
        do_stuff({"a": {"b": "d"}})
        assert executions == 4

    def test_cache_function_none(self):
        # global executions
        do_stuff_no_return()
        assert executions == 1
        do_stuff_no_return()
        assert executions == 1

    def test_cache_two_objects_with_cache_id(self):
        a = WithCacheIdOverride("a")
        a2 = WithCacheIdOverride("a")
        a2.i_am_different_now = "xyz"
        b = WithCacheIdOverride("b")

        a.do_stuff(1)
        assert executions == 1
        a2.do_stuff(1)
        assert executions == 1
        a2.do_stuff(2)
        assert executions == 2
        a.do_stuff(2)
        assert executions == 2

        b.do_stuff(1)
        assert executions == 3
        b.do_stuff(3)
        assert executions == 4
        a.do_stuff(3)
        assert executions == 5

    def test_cache_parallel(self):
        # POLY-231: where a cacheable functions runs in parallel it may try to cache twice

        thread_pool = concurrent.futures.ThreadPoolExecutor(2)

        t1 = thread_pool.submit(do_stuff_parallel, (1,))
        t2 = thread_pool.submit(do_stuff_parallel, (1,))

        t1.result()
        t2.result()
