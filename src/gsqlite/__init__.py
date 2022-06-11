import logging

from .connection import connect
from .exceptions import *


def get_version() -> str:
    conn = connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("select sqlite_version()")
    return cursor.fetchone()[0]


apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"
sqlite_version = get_version()
sqlite_version_info = tuple(int(v) for v in sqlite_version.split("."))


logging.basicConfig(level=logging.DEBUG)
