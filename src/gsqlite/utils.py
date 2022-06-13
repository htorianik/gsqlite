import ctypes
import logging
from typing import Callable

from .c_sqlite3 import libsqlite3
from .exceptions import DatabaseError


from .constants import (
    SQLITE_DONE,
    SQLITE_ROW,
    SQLITE_OK,
)


logger = logging.getLogger(__name__)


libsqlite3.sqlite3_errstr.restype = ctypes.c_char_p


def sqlite3_rc_guard(rc: int) -> None:
    SUCCESS_RCS = (
        SQLITE_DONE,
        SQLITE_ROW,
        SQLITE_OK
    )
    if rc not in SUCCESS_RCS:
        logger.debug("return code:%s", rc)
        errstr = libsqlite3.sqlite3_errstr(rc).decode()
        raise DatabaseError(errstr)


def sqlite3_call(
    func: Callable,
    *args,
):
    rc = func(*args)
    sqlite3_rc_guard(rc)
    return rc


def get_operation_command(opeartion: str) -> str:
    return opeartion.split()[0].upper()


def is_dql_opreation(operation: str) -> bool:
    """
    Checks if operation is a part Data Query Language (DQL).

    :param operation: SQL query.
    """
    return get_operation_command(operation) == "SELECT"

def is_dml_operation(operation: str) -> bool:
    """
    Checks if operation is a Data Manipulation Language (DML).

    :param operation: SQL query.
    """
    DML_COMMANDS = (
        "INSERT",
        "DELETE",
        "UPDATE",
        "CALL",
        "LOCK",
        "EXPLAIN_CALL",
    )
    return get_operation_command(operation) in DML_COMMANDS
