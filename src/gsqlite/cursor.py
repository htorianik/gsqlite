from .lastrowid import LastrowidMixin
from .description import DescriptionMixin
from .cursor_fetching_layer import CursorFetchingLayer


class Cursor(
    CursorFetchingLayer,
    DescriptionMixin,
    LastrowidMixin,
):
    def __init__(self, connection: "Connection"):
        super().__init__(connection)

        post_exec_hooks = (
            self._update_description,
            self._update_lastrowid,
        )

        for hook in post_exec_hooks:
            self._register_hook(hook)
