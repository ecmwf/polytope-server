"""
Test trusted metadata options merging in PolytopeDataSource.

Proves that:
1. Trusted metadata can override datacube and options per request
2. Client request fields (polytope_mars, metadata, pre_path, use_catalogue) do not influence config
3. Two sequential requests on one datasource use different metadata blocks
4. self.config is never mutated by metadata overlay
"""

import copy
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from polytope import PolytopeDataSource


class FakePolytopeMars:
    """Fake PolytopeMars that records what config it was initialized with."""

    def __init__(self, config, log_context=None):
        self.config = copy.deepcopy(config)
        self.log_context = log_context

    def extract(self, request):
        """Return a fake result including config details."""
        return {
            "datacube": self.config.get("datacube", {}),
            "options": self.config.get("options", {}),
            "request": request,
        }


class FakeRequest:
    """Minimal request object for testing."""

    def __init__(self, coerced_request, metadata=None):
        self.coerced_request = coerced_request
        self.metadata = metadata if metadata is not None else {}
        self.id = "test-request"
        self.user = FakeUser()


class FakeUser:
    """Minimal user object for testing."""

    def __init__(self):
        self.realm = "test"
        self.username = "test-user"


@pytest.fixture
def base_config():
    """Base worker config without metadata."""
    return {
        "type": "polytope",
        "datacube": {
            "type": "fdb",
            "config": "/tmp/test.yaml",
            "axis": ["class", "stream", "date"],
        },
        "options": {
            "pre_path": {},
            "use_catalogue": False,
            "engine_options": {},
            "axis_config": [
                {
                    "axis_name": "step",
                    "transformations": [{"name": "type_change", "type": "int"}],
                }
            ],
        },
        "gribjump_config": {
            "gribjump_servers": [{"host": "localhost", "port": 9999}]
        },
    }


@pytest.fixture
def mock_polytope_mars(monkeypatch):
    """Replace PolytopeMars with FakePolytopeMars."""
    # Mock the import location where PolytopeMars is used
    import sys
    import types
    
    # Create fake polytope_mars.api module
    fake_polytope_mars_api = types.ModuleType("polytope_mars.api")
    fake_polytope_mars_api.PolytopeMars = FakePolytopeMars
    
    # Create parent modules
    if "polytope_mars" not in sys.modules:
        sys.modules["polytope_mars"] = types.ModuleType("polytope_mars")
    sys.modules["polytope_mars.api"] = fake_polytope_mars_api
    
    # Create fake polytope_feature.utility.exceptions module
    fake_polytope_feature = types.ModuleType("polytope_feature")
    fake_polytope_feature_utility = types.ModuleType("polytope_feature.utility")
    fake_polytope_feature_utility_exceptions = types.ModuleType("polytope_feature.utility.exceptions")
    
    # Define a fake PolytopeError exception
    class PolytopeError(Exception):
        def __init__(self, message):
            self.message = message
            super().__init__(message)
    
    fake_polytope_feature_utility_exceptions.PolytopeError = PolytopeError
    
    sys.modules["polytope_feature"] = fake_polytope_feature
    sys.modules["polytope_feature.utility"] = fake_polytope_feature_utility
    sys.modules["polytope_feature.utility.exceptions"] = fake_polytope_feature_utility_exceptions
    
    yield
    
    # Cleanup
    for module_name in [
        "polytope_mars.api",
        "polytope_feature.utility.exceptions",
        "polytope_feature.utility",
        "polytope_feature",
    ]:
        if module_name in sys.modules:
            del sys.modules[module_name]


def test_metadata_datacube_overlay(base_config, mock_polytope_mars, tmp_path):
    """Test that metadata can override datacube structure."""
    # Setup temp config file
    config_file = tmp_path / "test_config.yaml"
    base_config["gribjump_config"] = {"gribjump_servers": []}

    datasource = PolytopeDataSource(base_config)
    original_config = copy.deepcopy(datasource.config)

    # Request with metadata that changes datacube
    request = FakeRequest(
        coerced_request={"class": "od", "stream": "oper"},
        metadata={
            "polytope_mars": {
                "datacube": {
                    "type": "fdb",
                    "config": "/tmp/different.yaml",
                    "axis": ["dataset", "date", "time"],
                }
            }
        },
    )

    timings = datasource.retrieve(request)

    # Verify the constructed config used the metadata datacube
    result = json.loads(datasource.output)
    assert result["datacube"]["axis"] == ["dataset", "date", "time"]
    assert result["datacube"]["config"] == "/tmp/different.yaml"

    # Verify self.config was not mutated
    assert datasource.config == original_config


def test_metadata_options_overlay(base_config, mock_polytope_mars, tmp_path):
    """Test that metadata can override options including pre_path, use_catalogue, engine_options."""
    base_config["gribjump_config"] = {"gribjump_servers": []}

    datasource = PolytopeDataSource(base_config)
    original_config = copy.deepcopy(datasource.config)

    # Request with metadata that changes options
    request = FakeRequest(
        coerced_request={"class": "od", "stream": "oper", "date": "20260101"},
        metadata={
            "polytope_mars": {
                "options": {
                    "pre_path": {"class": "od", "stream": "oper"},
                    "use_catalogue": True,
                    "engine_options": {"compact_result": True, "limit": 1000},
                    "axis_config": [
                        {
                            "axis_name": "date",
                            "transformations": [{"name": "type_change", "type": "date"}],
                        }
                    ],
                }
            }
        },
    )

    timings = datasource.retrieve(request)

    # Verify the constructed config used the metadata options
    result = json.loads(datasource.output)
    assert result["options"]["pre_path"] == {"class": "od", "stream": "oper"}
    assert result["options"]["use_catalogue"] is True
    assert result["options"]["engine_options"] == {"compact_result": True, "limit": 1000}
    assert len(result["options"]["axis_config"]) == 1
    assert result["options"]["axis_config"][0]["axis_name"] == "date"

    # Verify self.config was not mutated
    assert datasource.config == original_config


def test_client_request_fields_ignored(base_config, mock_polytope_mars, tmp_path):
    """Test that client request fields named metadata/polytope_mars/pre_path/use_catalogue do not influence config."""
    base_config["gribjump_config"] = {"gribjump_servers": []}

    datasource = PolytopeDataSource(base_config)

    # Request with malicious client-supplied fields trying to override config
    request = FakeRequest(
        coerced_request={
            "class": "od",
            "stream": "oper",
            # These client-supplied fields should be IGNORED for config purposes
            "metadata": {"polytope_mars": {"datacube": {"type": "evil"}}},
            "polytope_mars": {"datacube": {"type": "evil"}},
            "pre_path": {"malicious": "value"},
            "use_catalogue": "client_supplied",
            "datacube": {"type": "client_evil"},
        },
        metadata={
            "polytope_mars": {
                "options": {
                    "pre_path": {"class": "od"},
                    "use_catalogue": True,
                }
            }
        },
    )

    timings = datasource.retrieve(request)

    # Verify the config used ONLY trusted metadata, not client request fields
    result = json.loads(datasource.output)
    assert result["datacube"]["type"] == "fdb"  # Original, not "evil" or "client_evil"
    assert result["options"]["pre_path"] == {"class": "od"}  # From trusted metadata
    assert result["options"]["use_catalogue"] is True  # From trusted metadata

    # Verify the client request fields were passed through to extract but didn't alter config
    assert "metadata" in result["request"]
    assert "polytope_mars" in result["request"]
    assert "pre_path" in result["request"]


def test_two_sequential_requests_different_metadata(base_config, mock_polytope_mars, tmp_path):
    """Test that two sequential requests on one datasource use different metadata blocks."""
    base_config["gribjump_config"] = {"gribjump_servers": []}

    datasource = PolytopeDataSource(base_config)
    original_config = copy.deepcopy(datasource.config)

    # First request with metadata A
    request1 = FakeRequest(
        coerced_request={"class": "od", "stream": "oper"},
        metadata={
            "polytope_mars": {
                "datacube": {"type": "fdb", "axis": ["class", "stream"]},
                "options": {"pre_path": {"class": "od"}, "use_catalogue": False},
            }
        },
    )

    datasource.retrieve(request1)
    result1 = json.loads(datasource.output)

    # Verify first request used metadata A
    assert result1["datacube"]["axis"] == ["class", "stream"]
    assert result1["options"]["pre_path"] == {"class": "od"}
    assert result1["options"]["use_catalogue"] is False

    # Second request with metadata B
    request2 = FakeRequest(
        coerced_request={"dataset": "climate-dt", "date": "20260101"},
        metadata={
            "polytope_mars": {
                "datacube": {"type": "fdb", "axis": ["dataset", "date", "time"]},
                "options": {
                    "pre_path": {"dataset": "climate-dt"},
                    "use_catalogue": True,
                    "engine_options": {"compact": True},
                },
            }
        },
    )

    datasource.retrieve(request2)
    result2 = json.loads(datasource.output)

    # Verify second request used metadata B (completely different)
    assert result2["datacube"]["axis"] == ["dataset", "date", "time"]
    assert result2["options"]["pre_path"] == {"dataset": "climate-dt"}
    assert result2["options"]["use_catalogue"] is True
    assert result2["options"]["engine_options"] == {"compact": True}

    # Verify self.config was never mutated and is still the original
    assert datasource.config == original_config


def test_metadata_must_be_dict(base_config, mock_polytope_mars):
    """Test that non-dict metadata values raise an error."""
    base_config["gribjump_config"] = {"gribjump_servers": []}

    datasource = PolytopeDataSource(base_config)

    # Request with non-dict polytope_mars metadata
    request = FakeRequest(
        coerced_request={"class": "od"},
        metadata={"polytope_mars": "not_a_dict"},
    )

    with pytest.raises(ValueError, match="must be a dict"):
        datasource.retrieve(request)


def test_metadata_options_must_be_dict(base_config, mock_polytope_mars):
    """Test that non-dict options in metadata raise an error."""
    base_config["gribjump_config"] = {"gribjump_servers": []}

    datasource = PolytopeDataSource(base_config)

    # Request with non-dict options in metadata
    request = FakeRequest(
        coerced_request={"class": "od"},
        metadata={"polytope_mars": {"options": "not_a_dict"}},
    )

    with pytest.raises(ValueError, match="options.*must be a dict"):
        datasource.retrieve(request)


def test_no_metadata_uses_fallback_prepath(base_config, mock_polytope_mars):
    """Test that when no metadata is present, the legacy pre_path building logic runs."""
    base_config["gribjump_config"] = {"gribjump_servers": []}
    base_config["options"]["pre_path"] = []  # Will be popped in __init__

    datasource = PolytopeDataSource(base_config)
    # Manually set pre_path list for testing
    datasource.pre_path = ["class", "stream"]

    # Request with no metadata
    request = FakeRequest(
        coerced_request={
            "class": "od",
            "stream": "oper",
            "date": "20260101",
        },
        metadata={},  # No polytope_mars metadata
    )

    datasource.retrieve(request)
    result = json.loads(datasource.output)

    # Verify fallback pre_path logic ran and extracted class/stream from request
    assert result["options"]["pre_path"]["class"] == "od"
    assert result["options"]["pre_path"]["stream"] == "oper"


def test_change_grids_runs_after_metadata_overlay(base_config, mock_polytope_mars):
    """Test that change_grids() intra-dataset refinement runs after metadata overlay."""
    base_config["gribjump_config"] = {"gribjump_servers": []}
    base_config["gh69_fix_grids"] = True

    datasource = PolytopeDataSource(base_config)

    # Request with metadata AND a dataset that triggers change_grids
    request = FakeRequest(
        coerced_request={
            "class": "ng",
            "resolution": "standard",
            "date": "20260101",
        },
        metadata={
            "polytope_mars": {
                "options": {
                    "use_catalogue": True,
                    "axis_config": [
                        {
                            "axis_name": "step",
                            "transformations": [
                                {"name": "mapper", "type": "octahedral", "resolution": 64}
                            ],
                        }
                    ],
                }
            }
        },
    )

    datasource.retrieve(request)
    result = json.loads(datasource.output)

    # Verify change_grids ran and modified the mapper resolution to h128
    mapper_found = False
    for axis in result["options"]["axis_config"]:
        for trans in axis.get("transformations", []):
            if trans.get("name") == "mapper":
                mapper_found = True
                assert trans["resolution"] == 128  # change_grids should set this
    assert mapper_found, "mapper transformation should exist"


def test_preserved_trusted_metadata_namespaces(base_config, mock_polytope_mars):
    """Test that metadata overlay preserves other trusted metadata keys like cost, admin_overrides."""
    base_config["gribjump_config"] = {"gribjump_servers": []}

    # Add mock for request attributes that would carry other metadata
    request = FakeRequest(
        coerced_request={"class": "od"},
        metadata={
            "cost": 1500,
            "admin_overrides": {"bypass_rate_limit": True},
            "accept_encoding": ["gzip"],
            "buffer_full_output": False,
            "polytope_mars": {
                "options": {
                    "pre_path": {"class": "od"},
                }
            },
        },
    )

    datasource = PolytopeDataSource(base_config)

    # Just verify it doesn't break and uses the polytope_mars part correctly
    datasource.retrieve(request)
    result = json.loads(datasource.output)

    # Verify polytope_mars metadata was used
    assert result["options"]["pre_path"] == {"class": "od"}

    # Verify the request still has the other metadata (not stripped)
    assert request.metadata["cost"] == 1500
    assert request.metadata["admin_overrides"]["bypass_rate_limit"] is True


@pytest.fixture
def base_config_no_options():
    """Single-FE-pool base config that carries NO 'options' key at all.

    This mirrors the LUMI deployment, where every per-dataset datacube/options
    block is supplied at routing time via job metadata (set_metadata action),
    so the static pool config has only the shared gribjump/datacube scaffolding.
    """
    return {
        "type": "polytope",
        "datacube": {"type": "gribjump", "config": "/tmp/test.yaml"},
        "gribjump_config": {
            "gribjump_servers": [{"host": "localhost", "port": 9999}]
        },
    }


def test_metadata_options_when_base_has_no_options(
    base_config_no_options, mock_polytope_mars
):
    """Regression: a base config without 'options' must not KeyError, and a
    metadata options block carrying pre_path as a LIST of axis names must be
    converted to the per-request pre_path dict (previously raised
    KeyError: 'options', breaking every FE request on a single-pool deployment).
    """
    request = FakeRequest(
        {
            "class": "d1",
            "dataset": "on-demand-extremes-dt",
            "date": "20230820",
            "param": "146",
            "step": "0-10",
        },
        metadata={
            "polytope_mars": {
                "datacube": {"type": "gribjump"},
                "options": {
                    # pre_path is a LIST of axis names, as in the per-dataset
                    # datasource config ported into the set_metadata block.
                    "pre_path": ["class", "dataset", "date", "param"],
                    "use_catalogue": False,
                    "axis_config": [
                        {
                            "axis_name": "step",
                            "transformations": [{"name": "type_change", "type": "int"}],
                        }
                    ],
                },
            }
        },
    )

    datasource = PolytopeDataSource(base_config_no_options)
    # Must not raise KeyError: 'options'
    datasource.retrieve(request)
    result = json.loads(datasource.output)

    # pre_path list -> per-request dict for single-valued, in-list axes
    assert result["options"]["pre_path"] == {
        "class": "d1",
        "dataset": "on-demand-extremes-dt",
        "date": "20230820",
        "param": "146",
    }
    # non-pre_path axes (step) are not promoted into pre_path
    assert "step" not in result["options"]["pre_path"]
    # other metadata options survived the merge
    assert result["options"]["use_catalogue"] is False
    # base config is untouched (still no options key)
    assert "options" not in base_config_no_options


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
