import ctypes
import pathlib
import logging
import itertools
import functools

from typing import Tuple, Optional, Sequence, Union, TypeVar, List, Iterator

from .utils import sqlite3_rc_guard
from .c_sqlite3 import libsqlite3
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


class Cursor(
    Iterator[TRow],
    DescriptionMixin,
):

    closed: bool = False
    connection: "Connection"
    statement: Optional[ctypes.c_void_p] = None
    last_step_rc: Optional[int] = None
    row_iter: Iterator[TRow]

    __connection: "Connection"
    __lastrowid: Optional[int] = None

    arraysize: int = 1

    def __init__(self, connection: "Connection"):
        self.__connection = connection
        self.row_iter = iter(self)

    @property
    def connection(self) -> "Connection":
        return self.__connection

    @property
    def lastrowid(self):
        """
        Read-only attribute provides last id of the
        last inserted row after INSERT or REPLACE
        command. Execution of any other command will result
        in setting this attribute to `None`. Initial is `None`.
        """
        return self.__lastrowid

    def __update_lastrowid(self, operation: str):
        assert operation
        command = operation.split()[0].upper()

        if command not in ["INSERT", "REPLACE"]:
            self.__lastrowid = None
            return

        self.__lastrowid = libsqlite3.sqlite3_last_insert_rowid(
            self.connection.db,
        )

    @functools.lru_cache()
    def n_columns(self) -> int:
        return libsqlite3.sqlite3_data_count(self.statement)

    def __iter__(self) -> Iterator[TRow]:
        self.n_columns.cache_clear()
        return self

    def __next__(self) -> TRow:

        if self.last_step_rc == None or self.statement == None:
            raise StopIteration()

        if self.last_step_rc == SQLITE_DONE:
            self.__finalize()
            raise StopIteration

        row = tuple(
            get_column(self.statement, index) for index in range(self.n_columns())
        )

        self.last_step_rc = libsqlite3.sqlite3_step(self.statement)
        return row

    def execute(self, operation: str, params: TParams = ()):
        self.__not_closed_guard()
        self.__prepare(operation)

        for (index, value) in enumerate(params, 1):
            bind_param(self.statement, index, value)

        self.last_step_rc = libsqlite3.sqlite3_step(self.statement)
        self.row_iter = iter(self)

        self.__update_lastrowid(operation)

    def executemany(self, operation: str, seq_of_params: Sequence[TParams] = []):
        self.__not_closed_guard()
        self.__prepare(operation)

        for params in seq_of_params:
            for (index, value) in enumerate(params, 1):
                bind_param(self.statement, index, value)

            self.last_step_rc = libsqlite3.sqlite3_step(self.statement)
            self.__update_lastrowid(operation)
            libsqlite3.sqlite3_reset(self.statement)

        self.row_iter = iter(self)

    def fetchone(self) -> Optional[TRow]:
        self.__not_closed_guard()
        try:
            return next(self.row_iter)
        except StopIteration:
            return None

    def fetchmany(self, size: Optional[int] = None) -> List[TRow]:
        self.__not_closed_guard()
        return list(itertools.islice(self.row_iter, size or self.arraysize))

    def fetchall(self) -> List[TRow]:
        self.__not_closed_guard()
        return list(self.row_iter)

    def setinputsize(self):
        pass

    def setoutputsize(self):
        pass

    @property
    def rowcount(self) -> int:
        return -1

    def close(self):
        self.closed = True

    def __not_closed_guard(self) -> None:
        if self.connection.closed or self.closed:
            raise ProgrammingError(
                "Cannot operate on a closed database."
            )

    def __prepare(self, operation: str):
        self.__finalize()
        self.statement = ctypes.c_void_p()
        rc = libsqlite3.sqlite3_prepare_v2(
            self.connection.db,
            operation.encode(),
            -1,
            ctypes.byref(self.statement),
            ctypes.byref(ctypes.c_void_p()),
        )
        sqlite3_rc_guard(rc)

        self._update_description(self.statement, operation)

    def __finalize(self) -> None:
        self.last_step_rc = None
        self.row_iter = iter(self)

        if not self.statement:
            return

        libsqlite3.sqlite3_finalize(self.statement)
        self.statement = None
