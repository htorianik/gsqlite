import itertools
from typing import Optional, List

from .types import TRow
from .exceptions import ProgrammingError
from .cursor_execution_layer import CursorExecutionLayer


class CursorFetchingLayer(CursorExecutionLayer):
    """
    Class inheirts all public methods of `CursorExecutionLayer`
    and exposes `fetchone()`, `fetchmany()`, `fetchall()` methods
    and attribute `arraysize` that are specified in dbapi 2.
    """

    __arraysize: int = 1

    @property
    def arraysize(self) -> int:
        return self.__arraysize

    @arraysize.setter
    def arraysize(self, value: int):
        if not isinstance(value, int):
            raise ProgrammingError("Attribute arraysize must be int.")

        if value < 1:
            raise ProgrammingError("Attribute arraysize must be 1 or more.")

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