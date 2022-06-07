from unittest import TestCase

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

    def test_insert_executemany_params(self):
        users = [
            (0, "George", "Torianik"),
            (1, "Julia", "Tarasenko"),
            (2, "Solomia", "Panyok"),
        ]
        self.cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", users)
        self.cursor.execute("SELECT * FROM users")
        self.assertEqual(self.cursor.fetchall(), users)

    def test_fetchone(self):
        users = [
            (0, "George", "Torianik"),
            (1, "Julia", "Tarasenko"),
            (2, "Solomia", "Panyok"),
        ]
        self.cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", users)
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
        users = [
            (0, "George", "Torianik"),
            (1, "Julia", "Tarasenko"),
            (2, "Solomia", "Panyok"),
        ]
        self.cursor.executemany("INSERT INTO users VALUES (?, ?, ?)", users)
        self.assertListEqual(self.cursor.fetchall(), [])
        self.cursor.executemany("DELETE FROM users WHERE id=1")
        self.assertListEqual(self.cursor.fetchall(), [])
        self.cursor.executemany("UPDATE users SET id=3 WHERE id=0")
        self.assertListEqual(self.cursor.fetchall(), [])
