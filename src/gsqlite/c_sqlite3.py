import pathlib
import ctypes
import ctypes.util


libsqlite3_path = ctypes.util.find_library("sqlite3")
libsqlite3 = ctypes.CDLL(str(libsqlite3_path))


libsqlite3.sqlite3_column_name.restype = ctypes.c_char_p
