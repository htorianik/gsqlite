import ctypes
from typing import Union, Sequence, Callable


TElem = Union[int, float, str, bytes, None]
TRow = Sequence[TElem]
TParams = Sequence[TElem]

TPostExecutionHook = Callable[
    [
        "Connection", 
        ctypes.c_void_p, 
        str,
    ], 
    None,
]