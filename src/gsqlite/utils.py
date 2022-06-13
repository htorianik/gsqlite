import ctypes
import logging

from .c_sqlite3 import libsqlite3
from .exceptions import DatabaseError


logger = logging.getLogger(__name__)


libsqlite3.sqlite3_errstr.restype = ctypes.c_char_p


def sqlite3_rc_guard(rc: int) -> None:
    if rc != 0:
        logger.debug("return code:%s", rc)
        errstr = libsqlite3.sqlite3_errstr(rc).decode()
        raise DatabaseError(errstr)


def get_operation_command(opeartion: str) -> str:
    return opeartion.split()[0]


def is_dql_opreation(operation: str) -> str:
    """
    Checks if operation is a part Data Query Language (DQL).

    :operation: SQL operation.
    """
    return get_operation_command(operation) == "SELECT"
