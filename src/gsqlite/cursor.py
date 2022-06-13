import ctypes
import pathlib
import logging
import itertools
import functools
import dataclasses

from typing import Tuple, Optional, Sequence, Union, TypeVar, List, Iterator, Iterable, Callable

from .utils import sqlite3_rc_guard, sqlite3_call, is_dql_opreation, is_dml_operation
from .c_sqlite3 import libsqlite3
from .lastrowid import LastrowidMixin
from .exceptions import ProgrammingError
from .description import DescriptionMixin
from .types import *
from .cursor_iterator import CursorIterator
from .constants import (
    SQLITE3_TEXT,
    SQLITE_BLOB,
    SQLITE_FLOAT,
    SQLITE_INTEGER,
    SQLITE_NULL,
    SQLITE_TEXT,
    SQLITE_DONE,
    SQLITE_ROW,
)


logger = logging.getLogger(__name__)


def bind_param(
    statement: ctypes.c_void_p,
    index: int,
    value: TElem,
) -> None:
    """
    Binding to sqlite3_bind_*(). Support int, float, str, bytes, NoneType
    types of value.
    """
    if isinstance(value, int):
        rc = libsqlite3.sqlite3_bind_int(
            statement,
            index,
            value,
        )
    elif isinstance(value, float):
        rc = libsqlite3.sqlite3_bind_double(
            statement,
            index,
            value,
        )
    elif isinstance(value, str):
        rc = libsqlite3.sqlite3_bind_text(
            statement,
            index,
            ctypes.c_char_p(value.encode("utf-8")),
            len(value),
            ctypes.c_void_p(-1),  # Forces to copy the value
        )
    elif isinstance(value, bytes):
        rc = libsqlite3.sqlite3_bind_blob(
            statement,
            index,
            ctypes.c_void_p(value),
            len(value),
            ctypes.c_void_p(-1),  # Forces to copy the value
        )
    elif value == None:
        rc = libsqlite3.sqlite3_bind_null(statement, index)

    sqlite3_rc_guard(rc)



class CursorExecutionLayer(Iterable[TRow]):

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

    def close(self, operation: str):
        pass

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

    def __bind(self, params: TParams = ()):
        for (index, value) in enumerate(params, 1):
            bind_param(self.__statement, index, value)

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


class CursorFetchLayer(CursorExecutionLayer):

    __arraysize: int = 1

    @property
    def arraysize(self) -> int:
        return self.__arraysize

    @arraysize.setter
    def arraysize(self, value: int):
        if not isinstance(value, int):
            raise ProgrammingError("Attribute arraysize must be int.")

        if value < 1:
            raise ProgrammingError("Attribute arraysize must be 1 or more.")

        self.__arraysize = value

    def fetchone(self) -> Optional[TRow]:
        try:
            return next(iter(self))
        except StopIteration:
            return None

    def fetchmany(self, size: Optional[int] = None) -> List[TRow]:
        return list(
            itertools.islice(
                iter(self), size or self.arraysize
            )
        )

    def fetchall(self) -> List[TRow]:
        return list(iter(self))


class Cursor(
    CursorFetchLayer,
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
