import functools

from .cursor_lastrowid_mixin import CursorLastrowidMixin
from .cursor_description_mixin import CursorDescriptionMixin
from .cursor_fetching_layer import CursorFetchingLayer


class Cursor(
    CursorFetchingLayer,
    CursorDescriptionMixin,
    CursorLastrowidMixin,
):
    """
    Inheirts all public properties of CursorFetchingLayer
    as well as `description` and `lastrowid` attributes of mixins. 
    In addition defined `close()` method.
    """

    def __init__(self, connection: "Connection"):
        super().__init__(connection)

        post_exec_hooks = (
            self._update_description,
            self._update_lastrowid,
        )

        for hook in post_exec_hooks:
            self._register_hook(hook)

        self.__closed = False

    def close(self):
        self.__closed = True


def enforce_not_closed_wrapper(func):
    """
    Wrapping the function the way that ProgrammingError 
    is raised if either the current cursor or it's connection
    is closed.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.__closed or self.connection.closed:
            raise ProgrammingError("Cannot operate on a closed database.")

        return func(self, *args, **kwargs)
    return wrapper


# Wrapping every public method of Cursor with `enforce_not_closed_wrapper`
for name, obj in vars(Cursor).items():
    if callable(obj) and not name.startswith("_"):
        setattr(Cursor, name, enforce_not_closed_wrapper(obj))
