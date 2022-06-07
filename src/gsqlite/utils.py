import ctypes
import logging

from .c_sqlite3 import libsqlite3


logger = logging.getLogger(__name__)


libsqlite3.sqlite3_errstr.restype = ctypes.c_char_p


def sqlite3_rc_guard(rc: int) -> None:
    if rc != 0:
        logger.debug("return code:%s", rc)
        errstr = libsqlite3.sqlite3_errstr(rc).decode()
        raise Exception(errstr)
