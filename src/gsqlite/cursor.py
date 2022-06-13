from .cursor_lastrowid_mixin import CursorLastrowidMixin
from .cursor_description_mixin import CursorDescriptionMixin
from .cursor_fetching_layer import CursorFetchingLayer


class Cursor(
    CursorFetchingLayer,
    CursorDescriptionMixin,
    CursorLastrowidMixin,
):
    def __init__(self, connection: "Connection"):
        super().__init__(connection)

        post_exec_hooks = (
            self._update_description,
            self._update_lastrowid,
        )

        for hook in post_exec_hooks:
            self._register_hook(hook)
