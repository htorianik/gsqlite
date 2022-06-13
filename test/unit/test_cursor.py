from unittest import TestCase
from unittest.mock import ANY
from contextlib import contextmanager

from .utils import call_method

from src.gsqlite import connect, ProgrammingError


TEST_USERS = [
    (0, "George", "Torianik"),
    (1, "Julia", "Tarasenko"),
    (2, "Solomia", "Panyok"),
]


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


def users_testset_fixture(self):
    users_table_fixture(self)
    self.cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", TEST_USERS)
    return TEST_USERS


class TestCursorExecution(TestCase):

    def setUp(self):
        self.users = users_testset_fixture(self)

    def test_select_single_row(self):
        self.cursor.execute("SELECT 1")

        cursor_iter = iter(self.cursor)
        self.assertEqual(next(cursor_iter), (1,))
        
        with self.assertRaises(StopIteration):
            next(cursor_iter)

    def tests_insert_select_multiple_row(self):
        self.cursor.execute("SELECT * FROM users")
        self.assertSequenceEqual(list(self.cursor), self.users)

    def test_select_empty_cursor_no_exc(self):
        self.assertEqual(len(list(self.cursor)), 0)

    def test_executemany_dml_only(self):
        with self.assertRaises(ProgrammingError) as context:
            self.cursor.executemany("SELECT ?", ((1,), (2,)))

        self.assertIn(
            "executemany() can only execute DML statements.",
            str(context.exception),
        )

        with self.assertRaises(ProgrammingError) as context:
            self.cursor.executemany("CREATE TABLE foo (bar)", [None, None])

        self.assertIn(
            "executemany() can only execute DML statements.",
            str(context.exception),
        )


class TestCursorFetching(TestCase):

    def setUp(self):
        self.users = users_testset_fixture(self)

    def test_fetchone(self):
        self.cursor.execute("SELECT * FROM users")

        for i in range(len(self.users)):
            self.assertEqual(
                self.cursor.fetchone(),
                self.users[i],
            )

        for i in range(3):
            self.assertIsNone(self.cursor.fetchone())

    def test_fetchall_empty(self):
        self.assertEqual(len(self.cursor.fetchall()), 0)

    def test_fetchall(self):
        self.cursor.execute("SELECT * FROM users")
        self.assertEqual(self.cursor.fetchall(), self.users)

    def test_fetchall_second_is_empty(self):
        self.cursor.execute("SELECT * FROM users")
        self.assertEqual(self.cursor.fetchall(), self.users)
        self.assertEqual(len(self.cursor.fetchall()), 0)

    def test_arraysize_default_is_1(self):
        self.assertEqual(self.cursor.arraysize, 1)

    def test_arraysize_positive(self):
        with self.assertRaises(ProgrammingError) as context:
            self.cursor.arraysize = -1

        self.assertIn(
            "Attribute arraysize must be 1 or more.",
            str(context.exception),
        )

    def test_arraysize_int(self):
        with self.assertRaises(ProgrammingError) as context:
            self.cursor.arraysize = 12.3

        self.assertIn(
            "Attribute arraysize must be int.",
            str(context.exception),
        )

    def test_fetchmany_custom_arraysize(self):
        self.cursor.arraysize = 2
        self.cursor.execute("SELECT * FROM users")

        self.assertSequenceEqual(
            self.cursor.fetchmany(),
            self.users[:2],
        )

        self.assertSequenceEqual(
            self.cursor.fetchmany(),
            self.users[2:],
        )

        self.assertEqual(len(self.cursor.fetchmany()), 0)


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
        self.users = users_testset_fixture(self)

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
