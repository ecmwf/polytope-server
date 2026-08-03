#!/usr/bin/env python3
"""Regression tests for select-images.py's conservative dependency map."""

from __future__ import annotations

import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("select-images.py")
spec = importlib.util.spec_from_file_location("select_images", SCRIPT)
assert spec and spec.loader
select_images = importlib.util.module_from_spec(spec)
spec.loader.exec_module(select_images)


class SelectImagesTest(unittest.TestCase):
    def test_component_source_selects_its_image(self) -> None:
        self.assertEqual(
            select_images.selected_images(["workers/mars-worker/src/main.rs"]),
            {"mars-worker"},
        )

    def test_shared_worker_code_selects_every_worker(self) -> None:
        self.assertEqual(
            select_images.selected_images(["workers/common/src/lib.rs"]),
            select_images.WORKERS,
        )

    def test_observability_selects_all_consumers(self) -> None:
        self.assertEqual(
            select_images.selected_images(["observability/src/lib.rs"]),
            {"frontend", *select_images.WORKERS},
        )

    def test_lockfile_selects_every_image(self) -> None:
        self.assertEqual(
            select_images.selected_images(["Cargo.lock"]),
            select_images.ALL_IMAGES,
        )

    def test_documentation_selects_no_image(self) -> None:
        self.assertEqual(
            select_images.selected_images(["docs/mcp.md"]),
            set(),
        )


if __name__ == "__main__":
    unittest.main()
