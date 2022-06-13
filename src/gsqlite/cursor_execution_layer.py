"""
Module implement `CursorExecutionLayer` class.
"""

import ctypes
from typing import Iterable, Iterator, Optional, List, Sequence

from .types import TElem, TRow, TParams, TPostExecutionHook
from .c_sqlite3 import libsqlite3
from .utils import sqlite3_rc_guard, sqlite3_call, is_dml_operation, is_dql_opreation
from .cursor_iterator import CursorIterator
from .exceptions import ProgrammingError


class CursorExecutionLayer(Iterable[TRow]):
    """
    Class exposes `execute()`, `executemany()` methods and
    `connection` attribute of dbapi 2.0.

    Also, exposes iterator interface to fetch results after
    `execute()`.

    Provides `_register_hook()` method to post process
    the cursor's state after statement was executed.

    EXAMPLE:
    >>> c = CursorExecutionLayer(connection)
    >>> c.execute("CREATE TABLE foo (bar)")
    >>> c.executemany(
    ...     "INSERT INTO foo VALUES (?),
    ...     [(1,), (2,), (3,)]
    ... )
    >>> c.execute("SELECT * FROM foo")
    >>> for row in c:
    ...     print(row)
    (1,)
    (2,)
    (3,)
    """

    __connection: "Connection"
    __statement: Optional[ctypes.c_void_p] = None
    __operation: Optional[str] = None
    __iterator: Optional[Iterator[TRow]] = None
    __post_execution_hooks: List[TPostExecutionHook]

    def __init__(self, connection: "Connection"):
        self.__connection = connection
        self.__post_execution_hooks = []

    @property
    def connection(self):
        return self.__connection

    def execute(self, operation: str, params: TParams = ()):
        self.__prepare(operation)
        self.__bind(params)
        self.__iterator = CursorIterator.exec_and_iter(self.__statement)
        self.__call_hooks()

    def executemany(self, operation: str, seq_of_params: Sequence[TParams] = []):
        if not is_dml_operation(operation):
            raise ProgrammingError("executemany() can only execute DML statements.")

        self.__prepare(operation)
        for params in seq_of_params:
            self.__bind(params)
            sqlite3_call(libsqlite3.sqlite3_step, self.__statement)
            sqlite3_call(libsqlite3.sqlite3_reset, self.__statement)

        self.__iterator = None
        self.__call_hooks()

    def __iter__(self) -> Iterator[TRow]:
        if not self.__iterator:
            return iter([])

        return self.__iterator

    def _register_hook(self, hook: TPostExecutionHook):
        self.__post_execution_hooks.append(hook)

    def __call_hooks(self):
        for hook in self.__post_execution_hooks:
            hook(
                self.__connection,
                self.__statement,
                self.__operation,
            )

    def __prepare(self, statement: str):
        self.__finalize()
        self.__operation = statement
        self.__statement = ctypes.c_void_p()
        sqlite3_call(
            libsqlite3.sqlite3_prepare_v2,
            self.__connection.db,
            self.__operation.encode(),
            -1,
            ctypes.byref(self.__statement),
            ctypes.byref(ctypes.c_void_p()),
        )

    def __finalize(self) -> None:
        self.__iterator = None
        if self.__statement:
            sqlite3_call(
                libsqlite3.sqlite3_finalize,
                self.__statement,
            )
            self.__statement = None

    def __bind(self, params: TParams = ()):
        for (index, value) in enumerate(params, 1):
            self.__bind_param(index, value)

    def __bind_param(
        self,
        index: int,
        value: TElem,
    ) -> None:
        """
        Binding to sqlite3_bind_*(). 
        Supports int, float, str, bytes, NoneType.
        """
        if isinstance(value, int):
            sqlite3_call(
                libsqlite3.sqlite3_bind_int,
                self.__statement,
                index,
                value,
            )
        elif isinstance(value, float):
            sqlite3_call(
                libsqlite3.sqlite3_bind_double,
                self.__statement,
                index,
                value,
            )
        elif isinstance(value, str):
            sqlite3_call(
                libsqlite3.sqlite3_bind_text,
                self.__statement,
                index,
                ctypes.c_char_p(value.encode("utf-8")),
                len(value),
                ctypes.c_void_p(-1),  # Forces to copy the value
            )
        elif isinstance(value, bytes):
            sqlite3_call(
                libsqlite3.sqlite3_bind_blob,
                self.__statement,
                index,
                ctypes.c_void_p(value),
                len(value),
                ctypes.c_void_p(-1),  # Forces to copy the value
            )
        elif value == None:
            sqlite3_call(
                libsqlite3.sqlite3_bind_null,
                self.__statement,
                index,
            )
