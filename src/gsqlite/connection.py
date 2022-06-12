import ctypes

from .cursor import Cursor
from .utils import sqlite3_rc_guard
from .c_sqlite3 import libsqlite3
from .constants import SQLITE_OPEN_READWRITE
from .exceptions import ProgrammingError


class Connection:

    db: ctypes.c_void_p
    closed: bool = False

    def __init__(self, filename: str):
        self.db = ctypes.c_void_p()
        rc = libsqlite3.sqlite3_open(
            filename.encode(),
            ctypes.byref(self.db),
            SQLITE_OPEN_READWRITE,
        )
        sqlite3_rc_guard(rc)
        self.cursor().execute("BEGIN")

    def commit(self):
        self.__not_closed_guard()
        self.cursor().execute("COMMIT")

    def rollback(self):
        self.__not_closed_guard()
        self.cursor().execute("ROLLBACK")

    def cursor(self):
        self.__not_closed_guard()
        return Cursor(self)

    def close(self):
        self.__not_closed_guard()
        self.rollback()
        self.closed = True

    def __not_closed_guard(self):
        if self.closed:
            raise ProgrammingError(
                "Cannot operate on a closed database."
            )


def connect(dbpath: str = ":memory:") -> Connection:
    return Connection(dbpath)
