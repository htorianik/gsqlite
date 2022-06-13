"""
Module implements a `CursorIterator` class.
"""

import ctypes
from typing import Iterator

from .types import TElem, TRow
from .utils import sqlite3_call
from .c_sqlite3 import libsqlite3
from .constants import (
    SQLITE_DONE,
    SQLITE_INTEGER,
    SQLITE_FLOAT,
    SQLITE_BLOB,
    SQLITE3_TEXT,
    SQLITE_TEXT,
)


class CursorIterator(Iterator[TRow]):
    """
    Class to execute and iterate throught results of
    a prepared SQL statement.
    """

    __statement: ctypes.c_void_p
    __prev_step_rc: int
    __data_count: int

    def __init__(
        self,
        statement: ctypes.c_void_p,
        prev_step_rc: int,
    ):
        """
        Should not be used by the enduser.
        """
        self.__statement = statement
        self.__prev_step_rc = prev_step_rc
        self.__data_count = libsqlite3.sqlite3_data_count(self.__statement)

    @classmethod
    def exec_and_iter(cls, statement: ctypes.c_void_p) -> "CursorIterator":
        """
        Executes the statement, creates and returns an iterator through it's
        results.
        """
        prev_step_rc = sqlite3_call(libsqlite3.sqlite3_step, statement)
        return cls(statement, prev_step_rc)

    def __next__(self) -> TRow:
        if self.__prev_step_rc == SQLITE_DONE:
            raise StopIteration()

        row = tuple(self.__get_column(index) for index in range(self.__data_count))
        self.__prev_step_rc = sqlite3_call(
            libsqlite3.sqlite3_step,
            self.__statement,
        )
        return row

    def __get_column(self, index: int) -> TElem:
        column_type = libsqlite3.sqlite3_column_type(self.__statement, index)

        if column_type == SQLITE_INTEGER:
            return libsqlite3.sqlite3_column_int(self.__statement, index)

        if column_type == SQLITE_FLOAT:
            return libsqlite3.sqlite3_column_double(self.__statement, index)

        if column_type in [SQLITE_TEXT, SQLITE3_TEXT]:
            libsqlite3.sqlite3_column_text.restype = ctypes.c_char_p
            column_bytes = libsqlite3.sqlite3_column_text(self.__statement, index)
            return column_bytes.decode("utf-8")

        if column_type == SQLITE_NULL:
            return None

        raise ProgrammingError(
            f"Retrieving a column of type {column_type} is not implemented."
        )
