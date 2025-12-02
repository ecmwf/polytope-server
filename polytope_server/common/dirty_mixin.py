class DirtyTrackingMixin:
    __slots__ = ("_dirty_fields",)

    def __setattr__(self, key, value):
        if key == "_dirty_fields":
            super().__setattr__(key, value)
            return

        if not hasattr(self, "_dirty_fields"):
            object.__setattr__(self, "_dirty_fields", set())

        # Check if the value actually changed could be an optimization,
        # but for now just track assignment.
        # To check equality we would need to get the old value, which might not exist.
        try:
            old_value = getattr(self, key)
            if old_value != value:
                self._dirty_fields.add(key)
        except AttributeError:
            # First assignment
            self._dirty_fields.add(key)

        super().__setattr__(key, value)

    def mark_dirty(self, key):
        if not hasattr(self, "_dirty_fields"):
            object.__setattr__(self, "_dirty_fields", set())
        self._dirty_fields.add(key)

    def get_dirty_fields(self):
        if not hasattr(self, "_dirty_fields"):
            return set()
        return self._dirty_fields

    def clear_dirty(self):
        if hasattr(self, "_dirty_fields"):
            self._dirty_fields.clear()
