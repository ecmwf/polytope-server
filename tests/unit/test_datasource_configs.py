import pytest
from polytope_server.common.datasource.datasource import get_datasource_config
from polytope_server.common.config import polytope_config


def test_merge_parents(monkeypatch):
    global_config = {
        "datasources": {
            "base": {"type": "dummy", "defaults": {"a": 1}},
            "child": {"type": "dummy", "parents": ["base"], "defaults": {"b": 2}},
            "grandchild": {"type": "dummy", "parents": ["child"], "defaults": {"c": 3}},
        }
    }

    monkeypatch.setattr(polytope_config, "global_config", global_config)
    config = get_datasource_config("child")
    # Should merge defaults from base and child
    assert config["defaults"]["a"] == 1
    assert config["defaults"]["b"] == 2

    config2 = get_datasource_config("grandchild")
    # Should merge all three levels
    assert config2["defaults"]["a"] == 1
    assert config2["defaults"]["b"] == 2
    assert config2["defaults"]["c"] == 3


def test_chain_order(monkeypatch):
    global_config = {
        "datasources": {
            "valid_chain_1": {"type": "dummy", "defaults": {"d": 4, "overwrite": 0}, "other": [1]},
            "valid_chain_2": {"type": "dummy", "defaults": {"e": 5, "overwrite": 1}, "other": [2]},
            "valid_chain_3": {
                "type": "dummy",
                "parents": ["valid_chain_1", "valid_chain_2"],
                "defaults": {"f": 6},
                "other": [3],
            },
        }
    }

    monkeypatch.setattr(polytope_config, "global_config", global_config)
    config = get_datasource_config("valid_chain_3")
    assert config["defaults"] == {"d": 4, "e": 5, "f": 6, "overwrite": 1}
    assert config["other"] == [1, 2, 3]

    # test that latest config overwrites
    config = get_datasource_config("valid_chain_3")
    config["defaults"]["overwrite"] = 2
    config = get_datasource_config(config)
    assert config["defaults"]["overwrite"] == 2


def test_recursive_parent_detection(monkeypatch):
    global_config = {
        "datasources": {
            "cycle": {"type": "dummy", "parents": ["cycle"]},
            "cycle1": {"type": "dummy", "parents": ["cycle3"]},
            "cycle2": {"type": "dummy", "parents": ["cycle1"]},
            "cycle3": {"type": "dummy", "parents": ["cycle2"]},
        }
    }

    monkeypatch.setattr(polytope_config, "global_config", global_config)
    # Direct cycle
    with pytest.raises(KeyError):
        get_datasource_config("cycle")
    # Indirect cycle
    with pytest.raises(KeyError):
        get_datasource_config("cycle2")
    with pytest.raises(KeyError):
        get_datasource_config("cycle1")
    with pytest.raises(KeyError):
        get_datasource_config("cycle3")
