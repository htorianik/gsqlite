import sqlite3
from pathlib import Path
from unittest import TestCase

from src import gsqlite


def test_threadsafety():
    assert gsqlite.threadsafety == 1


def skip_test_isolation(tmpdir: Path):
    db_filename = str(tmpdir / "test.sqlite")
    conn1 = sqlite3.connect(db_filename)
    conn2 = sqlite3.connect(db_filename)
    cursor1 = conn1.cursor()
    cursor2 = conn2.cursor()

    cursor1.execute("CREATE TABLE users (id, name)")
    cursor1.execute("COMMIT")

    cursor1.executemany(
        "INSERT INTO users VALUES (?, ?)", 
        [
            (1, "George"),
            (2, "Solomia"),
        ]
    )
    cursor1.execute("ROLLBACK")

    cursor2.execute("SELECT * FROM users")
    assert len(cursor2.fetchall()) == 0

    conn1.commit()

    cursor2.execute("SELECT * FROM users")
    assert len(cursor2.fetchall()) == 1
