from unittest import TestCase
from unittest.mock import ANY
from contextlib import contextmanager

from .utils import call_method

from src.gsqlite import connect, ProgrammingError


def cursor_fixture(self):
    self.connection = connect(":memory:")
    self.cursor = self.connection.cursor()


def users_table_fixture(self):
    cursor_fixture(self)
    self.cursor.execute(
        "CREATE TABLE users ("
        "id INT NOT NULL,"
        "name VAR(255) NOT NULL,"
        "surname VAR(255) NOT NULL)"
    )

def users_set_1_fixture(self):
    users_table_fixture(self)
    users = [
        (0, "George", "Torianik"),
        (1, "Julia", "Tarasenko"),
        (2, "Solomia", "Panyok"),
    ]
    self.cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", users)
    return users


class TestCursor(TestCase):
    def setUp(self):
        users_table_fixture(self)

    def tearDown(self):
        self.connection.close()
        self.cursor.close()

    def test_select_no_rows(self):
        self.cursor.execute("SELECT * FROM users")
        self.assertEqual(len(self.cursor.fetchall()), 0)

    def test_insert_select_basic(self):
        """
        Tests if two single insert statatements with no params gives
        """
        self.cursor.execute("INSERT INTO users VALUES (0, 'George', 'Torianik')")
        self.cursor.execute("INSERT INTO users VALUES (1, 'Solomia', 'Panyok')")
        self.cursor.execute("SELECT * FROM users")
        self.assertListEqual(
            self.cursor.fetchall(),
            [
                (0, "George", "Torianik"),
                (1, "Solomia", "Panyok"),
            ],
        )

    def test_insert_executemany_params(self):
        users = users_set_1_fixture(self)
        self.cursor.execute("SELECT * FROM users")
        self.assertEqual(self.cursor.fetchall(), users)

    def test_fetchone(self):
        users = users_set_1_fixture(self)
        self.cursor.execute("SELECT * FROM users")
        self.assertEqual(self.cursor.fetchone(), users[0])
        self.assertEqual(self.cursor.fetchone(), users[1])
        self.assertEqual(self.cursor.fetchone(), users[2])
        self.assertEqual(self.cursor.fetchone(), None)
        self.assertEqual(self.cursor.fetchone(), None)

    def test_dml_empty_cursor(self):
        """
        Checks if nothing to fetch from cursor after
        a DML execution.
        """
        users_set_1_fixture(self)
        self.assertListEqual(self.cursor.fetchall(), [])
        self.cursor.executemany("DELETE FROM users WHERE id=1")
        self.assertListEqual(self.cursor.fetchall(), [])
        self.cursor.executemany("UPDATE users SET id=3 WHERE id=0")
        self.assertListEqual(self.cursor.fetchall(), [])
    

class TestCursorDescription(TestCase):

    def setUp(self):
        users_table_fixture(self)

    def test_description_readonly(self):
        with self.assertRaises(AttributeError):
            self.cursor.description = "132"

    def test_description_default_none(self):
        self.assertIsNone(self.cursor.description)

    def test_description_select(self):
        self.cursor.execute("SELECT id, surname, 1 as const_field FROM users")
        self.assertSequenceEqual(
            self.cursor.description,
            [
                ("id", ANY, ANY, ANY, ANY, ANY, ANY),
                ("surname", ANY, ANY, ANY, ANY, ANY, ANY),
                ("const_field", ANY, ANY, ANY, ANY, ANY, ANY),
            ]
        )

    def test_description_no_select(self):
        self.cursor.execute("SELECT id, surname, 1 as const_field FROM users")
        self.cursor.execute("DELETE FROM users WHERE 1=2")
        self.assertIsNone(self.cursor.description)


class TestCursorLastrowid(TestCase):

    def setUp(self):
        self.users = users_set_1_fixture(self)

    def test_lastrowid_readonly(self):
        with self.assertRaises(AttributeError):
            self.cursor.lastrowid = 456

    def test_lastrowid_insert(self):
        self.cursor.execute(
            "INSERT INTO users VALUES (?, ?, ?)", 
            (1, "George", "Torianik")
        )
        self.assertEqual(self.cursor.lastrowid, 4)

    def test_lastrowid_no_insert_neither_replace(self):
        self.cursor.execute(
            "INSERT INTO users VALUES (?, ?, ?)", 
            (1, "George", "Torianik")
        )
        self.cursor.execute("SELECT * FROM users")
        self.assertIsNone(self.cursor.lastrowid)


class TestCursorConnection(TestCase):

    CURSOR_PUBLIC_METHODS = (
        "execute",
        "executemany",
        "fetchone",
        "fetchmany",
        "fetchall",
    )

    CONNECTION_PUBLIC_METHOD = (
        "commit",
        "rollback",
        "cursor",
        "close",
    )

    def setUp(self):
        self.conn = connect(":memory:")
        self.cursor1 = self.conn.cursor()
        self.cursor2 = self.conn.cursor()

    def test_conn_read_only(self):
        with self.assertRaises(AttributeError):
            self.cursor1.connection = None

    def test_close_conn(self):
        self.conn.close()
        for public_method in self.CONNECTION_PUBLIC_METHOD:
            with self.assertRaisesProgrammingClosedError():
                call_method(self.conn, public_method)

    def test_close_cursor(self):
        self.cursor1.close()
        with self.assertRaisesProgrammingClosedError():
            self.cursor1.execute("SELECT 1")

        self.cursor2.execute("SELECT 2")
        self.conn.cursor()

    @contextmanager
    def assertRaisesProgrammingClosedError(self):
        try:
            yield
        except ProgrammingError as exc:
            self.assertTrue(
                "Cannot operate on a closed database." in str(exc)
            )
        else:
            assert False, "statement doesn't raise a ProgrammingError"
