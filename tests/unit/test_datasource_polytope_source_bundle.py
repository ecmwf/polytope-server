import json
import os
import sys
from pathlib import Path

import polytope_server.common.datasource.polytope as polytope_module
from polytope_server.common.datasource.polytope import (
    PolytopeDataSource,
    _python_site_packages,
)


def _base_config(bundle_root: Path) -> dict:
    return {
        "type": "polytope",
        "name": "polytope-source-test",
        "options": {"pre_path": []},
        "datacube": {"type": "gribjump"},
        "gribjump_config": {"servermap": [], "threads": 1},
        "fdb_config": {"type": "local", "engine": "toc", "schema": "dummy"},
        "source_bundle_root": str(bundle_root),
    }


def _make_request():
    return type(
        "Request",
        (),
        {
            "coerced_request": {"feature": {"type": "point"}},
            "user": type("User", (), {"realm": "realm", "username": "user"})(),
            "id": "req-1",
        },
    )()


def _prepare_bundle(bundle_root: Path) -> Path:
    site_packages = _python_site_packages(bundle_root)
    site_packages.mkdir(parents=True)
    return site_packages


def _fake_polytope_mars_class():
    class FakePolytopeMars:
        def __init__(self, config, log_context):
            self.marker = Path(os.environ["GRIBJUMP_HOME"]).name

        def extract(self, request):
            return {
                "marker": self.marker,
                "gribjump_home": os.environ.get("GRIBJUMP_HOME"),
                "fdb_home": os.environ.get("FDB_HOME"),
            }

    return FakePolytopeMars


def test_polytope_datasource_resolves_sequential_source_bundles(monkeypatch, tmp_path):
    monkeypatch.setenv("GRIBJUMP_HOME", "/existing/gribjump")
    monkeypatch.setenv("FDB_HOME", "/existing/fdb")
    monkeypatch.setenv("FDB5_HOME", "/existing/fdb5")
    monkeypatch.setenv("GRIBJUMP_DIR", "/existing/gribjump-dir")
    monkeypatch.setenv("FDB5_DIR", "/existing/fdb5-dir")
    monkeypatch.setenv("ECCODES_DIR", "/existing/eccodes-dir")
    monkeypatch.setenv("FINDLIBS_DISABLE_PACKAGE", "no")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/existing/lib")
    monkeypatch.setenv("GRIBJUMP_CONFIG_FILE", "/existing/gribjump.yaml")
    monkeypatch.setenv("FDB5_CONFIG_FILE", "/existing/fdb.yaml")
    monkeypatch.delitem(sys.modules, "polytope_mars", raising=False)
    monkeypatch.delitem(sys.modules, "polytope_mars.api", raising=False)
    monkeypatch.setattr(
        polytope_module,
        "_import_polytope_mars",
        _fake_polytope_mars_class,
    )

    bundle_a = tmp_path / "bundle-a"
    bundle_b = tmp_path / "bundle-b"
    site_packages_a = _prepare_bundle(bundle_a)
    site_packages_b = _prepare_bundle(bundle_b)

    ds_a = PolytopeDataSource(_base_config(bundle_a))
    assert ds_a.config_file != ds_a.fdb_config_file
    assert ds_a.config_file != "/tmp/gribjump.yaml"
    assert ds_a.fdb_config_file != "/tmp/fdb.yaml"
    assert str(site_packages_a) in sys.path

    assert ds_a.retrieve(_make_request()) is True
    assert json.loads(ds_a.output.decode("utf-8"))["marker"] == "bundle-a"
    assert os.environ["GRIBJUMP_HOME"] == str(bundle_a)
    assert os.environ["FDB_HOME"] == str(bundle_a)
    assert os.environ["FDB5_HOME"] == str(bundle_a)
    assert os.environ["GRIBJUMP_DIR"] == str(bundle_a)
    assert os.environ["FDB5_DIR"] == str(bundle_a)
    assert os.environ["ECCODES_DIR"] == str(bundle_a)
    assert os.environ["FINDLIBS_DISABLE_PACKAGE"] == "yes"
    assert os.environ["LD_LIBRARY_PATH"] == f"{bundle_a / 'lib'}:/existing/lib"

    ds_a.destroy(None)
    assert str(site_packages_a) not in sys.path
    assert os.environ["GRIBJUMP_HOME"] == "/existing/gribjump"
    assert os.environ["FDB_HOME"] == "/existing/fdb"
    assert os.environ["FDB5_HOME"] == "/existing/fdb5"
    assert os.environ["GRIBJUMP_DIR"] == "/existing/gribjump-dir"
    assert os.environ["FDB5_DIR"] == "/existing/fdb5-dir"
    assert os.environ["ECCODES_DIR"] == "/existing/eccodes-dir"
    assert os.environ["FINDLIBS_DISABLE_PACKAGE"] == "no"
    assert os.environ["LD_LIBRARY_PATH"] == "/existing/lib"
    assert os.environ["GRIBJUMP_CONFIG_FILE"] == "/existing/gribjump.yaml"
    assert os.environ["FDB5_CONFIG_FILE"] == "/existing/fdb.yaml"

    ds_b = PolytopeDataSource(_base_config(bundle_b))
    assert str(site_packages_b) in sys.path

    assert ds_b.retrieve(_make_request()) is True
    assert json.loads(ds_b.output.decode("utf-8"))["marker"] == "bundle-b"

    ds_b.destroy(None)
    assert str(site_packages_b) not in sys.path


def test_polytope_datasource_creates_unique_temp_files(tmp_path):
    bundle_root = tmp_path / "bundle"
    _prepare_bundle(bundle_root)

    ds_a = PolytopeDataSource(_base_config(bundle_root))
    ds_b = PolytopeDataSource(_base_config(bundle_root))

    assert ds_a.config_file != ds_b.config_file
    assert ds_a.fdb_config_file != ds_b.fdb_config_file
    assert Path(ds_a.config_file).exists()
    assert Path(ds_b.config_file).exists()
    assert Path(ds_a.fdb_config_file).exists()
    assert Path(ds_b.fdb_config_file).exists()

    ds_a.destroy(None)
    assert not Path(ds_a.config_file).exists()
    assert not Path(ds_a.fdb_config_file).exists()
    assert Path(ds_b.config_file).exists()
    assert Path(ds_b.fdb_config_file).exists()

    ds_b.destroy(None)


def test_polytope_datasource_without_source_bundle_does_not_set_bundle_env(monkeypatch, tmp_path):
    config = _base_config(tmp_path)
    config.pop("source_bundle_root")

    monkeypatch.delenv("GRIBJUMP_HOME", raising=False)
    monkeypatch.delenv("FDB_HOME", raising=False)
    monkeypatch.delenv("FDB5_HOME", raising=False)
    monkeypatch.delenv("GRIBJUMP_DIR", raising=False)
    monkeypatch.delenv("FDB5_DIR", raising=False)
    monkeypatch.delenv("ECCODES_DIR", raising=False)
    monkeypatch.delenv("FINDLIBS_DISABLE_PACKAGE", raising=False)
    monkeypatch.delenv("LD_LIBRARY_PATH", raising=False)

    ds = PolytopeDataSource(config)

    assert "GRIBJUMP_HOME" not in os.environ
    assert "FDB_HOME" not in os.environ
    assert "FDB5_HOME" not in os.environ
    assert "GRIBJUMP_DIR" not in os.environ
    assert "FDB5_DIR" not in os.environ
    assert "ECCODES_DIR" not in os.environ
    assert "FINDLIBS_DISABLE_PACKAGE" not in os.environ
    assert "LD_LIBRARY_PATH" not in os.environ

    ds.destroy(None)
