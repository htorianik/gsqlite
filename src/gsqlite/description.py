"""
Module provides mixin to expose `Cursor.description`
propery of dbapi 2.0 spec.
"""

import ctypes
from typing import Tuple, Sequence, Optional

from .c_sqlite3 import libsqlite3
from .utils import is_dql_opreation


TDescriptionColumn = Tuple[str, None, None, None, None, None, None]
TDescription = Sequence[TDescriptionColumn]


class DescriptionMixin:

    __description: Optional[TDescription] = None

    @property
    def description(self) -> Optional[TDescription]:
        """
        Read-only attribute provides column names for
        the last query. If no query was executed or operation
        didn't retur any row then the value is `None`.

        For every column it returns 7-row tuple where the
        first element is a name and the last 6 are empty.
        """
        return self.__description

    def _update_description(
        self,
        connection: "Connection",
        statement: ctypes.c_void_p,
        operation: str,
    ):
        """
        Protected method to call when description may change
        e.g. after sqlite3_prepare() on SELECT operation.

        :param connection: Connection where the statement was executed.
        :param statement: Executed statement.
        :param operation: SQL statement that was executed.
        """
        if not is_dql_opreation(operation):
            self.__description = None
            return

        column_count = libsqlite3.sqlite3_column_count(statement)
        column_names = (
            libsqlite3.sqlite3_column_name(statement, n).decode("utf-8")
            for n in range(column_count)
        )

        self.__description = tuple(
            (name, None, None, None, None, None, None)
            for name in column_names
        )
