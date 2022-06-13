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


TElem = Union[int, float, str, bytes, None]
TRow = Sequence[TElem]
TParams = Sequence[TElem]


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


def get_column(statement: ctypes.c_void_p, index: int) -> TElem:
    column_type = libsqlite3.sqlite3_column_type(statement, index)

    if column_type == SQLITE_INTEGER:
        return libsqlite3.sqlite3_column_int(statement, index)

    if column_type == SQLITE_FLOAT:
        return libsqlite3.sqlite3_column_double(statement, index)

    if column_type in [SQLITE_TEXT, SQLITE3_TEXT]:
        libsqlite3.sqlite3_column_text.restype = ctypes.c_char_p
        return libsqlite3.sqlite3_column_text(statement, index).decode("utf-8")

    if column_type == SQLITE_NULL:
        return None

    raise NotImplementedError()


class CursorIterator(Iterator[TRow]):
    """
    Class responsible for executing prepared SQL statement with binded
    parameters and then fetching results.
    """

    __statement: ctypes.c_void_p
    __prev_step_rc: int
    __data_count: int

    def __init__(
        self, 
        statement: ctypes.c_void_p,
        prev_step_rc: int,
    ):
        self.__statement = statement
        self.__prev_step_rc = prev_step_rc
        self.__data_count = libsqlite3.sqlite3_data_count(self.__statement)

    @classmethod
    def exec_and_iter(cls, statement: ctypes.c_void_p) -> "CursorIterator":
        prev_step_rc = sqlite3_call(libsqlite3.sqlite3_step, statement)
        return cls(statement, prev_step_rc)

    def __next__(self) -> TRow:
        if self.__prev_step_rc == SQLITE_DONE:
            raise StopIteration()

        row = tuple(
            get_column(self.__statement, index)
            for index in range(self.__data_count)
        )
        self.__prev_step_rc = sqlite3_call(
            libsqlite3.sqlite3_step, 
            self.__statement,
        )
        return row



TPostExecutionHook = Callable[
    [
        "Connection", 
        ctypes.c_void_p, 
        str,
    ], 
    None,
]


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

    def __reset(self):
        self.__iterator = None
        sqlite3_call(
            libsqlite3.sqlite3_reset,
            self.__statement,
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
            raise TypeError("Attribute arraysize must be int.")

        if value < 1:
            raise ValueError("Attribute arraysize must be 1 or more.")

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
