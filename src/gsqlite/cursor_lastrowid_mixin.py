"""
Implements `CursorLastrowidMixin` class.
"""

import ctypes
from typing import Optional

from .utils import get_operation_command
from .c_sqlite3 import libsqlite3


class CursorLastrowidMixin:
    """
    Exposes `lastrowid` attribute specified by dbapi 2.0.

    Can be used as an extension to a subclass of
    `cursor_execution_layer.CursorExecutionLayer` by 
    defining subclass of it with this mixin and registering
    `_update_lastrowid` with `CursorExecutionLayer_register_hook`.
    """

    __lastrowid: Optional[int] = None

    @property
    def lastrowid(self):
        """
        Read-only attribute provides last id of the
        last inserted row after INSERT or REPLACE
        command. Execution of any other command will result
        in setting this attribute to `None`. Initial is `None`.
        """
        return self.__lastrowid

    def _update_lastrowid(
        self, 
        connection: "Connection",
        statement: ctypes.c_void_p,
        operation: str
    ):
        """
        Protected method to call after a INSERT or REPLACE
        statement was executed to update the lastrowid.

        :param connection: Connection where the statement was executed.
        :param statement: Executed statement.
        :param operation: SQL statement that was executed.
        """
        if get_operation_command(operation) not in ["INSERT", "REPLACE"]:
            self.__lastrowid = None
            return

        self.__lastrowid = libsqlite3.sqlite3_last_insert_rowid(connection.db)
