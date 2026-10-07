import json
import sys
from pathlib import Path

import pytest

WORKER_DIR = Path(__file__).resolve().parents[1]
if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

from feature_resolve import (  # type: ignore[import-not-found]  # noqa: E402
    FeatureResolveError,
    normalise_result,
 )


def test_healpix_nested_points_are_sorted_deduplicated_and_coalesced():
    # Indices/centres captured from a small H1024 nested polytope slice.  Input
    # deliberately mirrors union overlap and slicer traversal order.
    rows = [
        (74423, 13.401350554355, 52.5146484375),
        (74398, 13.324670581098, 52.4267578125),
        (74399, 13.324670581098, 52.5146484375),
        (74423, 13.401350554355, 52.5146484375),
    ]
    result = normalise_result(
        "polygon",
        rows,
        12 * 1024 * 1024,
        100,
        computed_ranges=((74398, 74400), (74423, 74424)),
    )
    assert result == {
        "type": "polygon",
        "n_points": 3,
        "ranges": [[74398, 74400], [74423, 74424]],
        "coords": {
            "lat": [13.324670581098, 13.324670581098, 13.401350554355],
            "lon": [52.4267578125, 52.5146484375, 52.5146484375],
        },
    }
    json.dumps(result)


def test_empty_cap_and_gribjump_mismatch_errors_are_clear():
    with pytest.raises(FeatureResolveError, match="selects no grid points"):
        normalise_result("boundingbox", [], 100, 10)
    with pytest.raises(FeatureResolveError, match="max_feature_points=1"):
        normalise_result("polygon", [(1, 0.0, 0.0), (2, 0.0, 1.0)], 100, 1)
    with pytest.raises(FeatureResolveError, match="computed GribJump ranges"):
        normalise_result("polygon", [(1, 0.0, 0.0)], 100, 10, ((2, 3),))
