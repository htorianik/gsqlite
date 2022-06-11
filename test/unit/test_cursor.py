from unittest import TestCase
from unittest.mock import ANY

from src.gsqlite import connect


class TestCursor(TestCase):
    def setUp(self):
        self.connection = connect(":memory:")
        self.cursor = self.connection.cursor()
        self.cursor.execute(
            "CREATE TABLE users ("
            "id INT NOT NULL,"
            "name VAR(255) NOT NULL,"
            "surname VAR(255) NOT NULL)"
        )

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

    def __user_set_1(self):
        users = [
            (0, "George", "Torianik"),
            (1, "Julia", "Tarasenko"),
            (2, "Solomia", "Panyok"),
        ]
        self.cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", users)
        return users

    def test_insert_executemany_params(self):
        users = self.__user_set_1()
        self.cursor.execute("SELECT * FROM users")
        self.assertEqual(self.cursor.fetchall(), users)

    def test_fetchone(self):
        users = self.__user_set_1()
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
        self.__user_set_1()
        self.assertListEqual(self.cursor.fetchall(), [])
        self.cursor.executemany("DELETE FROM users WHERE id=1")
        self.assertListEqual(self.cursor.fetchall(), [])
        self.cursor.executemany("UPDATE users SET id=3 WHERE id=0")
        self.assertListEqual(self.cursor.fetchall(), [])

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
    