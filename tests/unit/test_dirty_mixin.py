import pytest

from polytope_server.common.dirty_mixin import DirtyTrackingMixin
from polytope_server.common.request import PolytopeRequest, Status


class TestDirtyTrackingMixin:

    def test_slots_separation(self):
        """Test that PolytopeRequest slots do not include _dirty_fields"""
        # PolytopeRequest defines its own __slots__
        # DirtyTrackingMixin defines its own __slots__

        req_slots = PolytopeRequest.__slots__
        assert "_dirty_fields" not in req_slots

        mixin_slots = DirtyTrackingMixin.__slots__
        assert "_dirty_fields" in mixin_slots

    def test_dirty_tracking_lifecycle(self):
        """Test the lifecycle of dirty tracking"""
        req = PolytopeRequest()

        # Initially, all dirty fields
        assert len(req.get_dirty_fields()) == len(req.__slots__)
        req.clear_dirty()
        assert len(req.get_dirty_fields()) == 0

        # Change a field
        req.user_message = "new message"
        assert {"user_message"} == req.get_dirty_fields()

        # Serialize should not include _dirty_fields
        serialized = req.serialize()
        assert "_dirty_fields" not in serialized

        # Check that the mixin doesn't break the object structure
        # i.e. it doesn't create a __dict__
        with pytest.raises(AttributeError):
            _ = req.__dict__

    def test_status_history_dirty(self):
        """Test that status_history is marked dirty when status changes"""
        req = PolytopeRequest()
        req.clear_dirty()

        # Change status
        req.set_status(Status.PROCESSING)

        dirty = req.get_dirty_fields()
        assert "status" in dirty
        assert "status_history" in dirty

        # Verify status_history content
        assert Status.PROCESSING.value in req.status_history

    def test_mark_dirty_explicit(self):
        """Test explicit mark_dirty"""
        req = PolytopeRequest()
        req.clear_dirty()

        req.mark_dirty("user_message")
        assert "user_message" in req.get_dirty_fields()
