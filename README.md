# gsqlite

## Abstract
DBAPI 2.0 implementation for sqlite3.

## Example
```python3
>>> from gsqlite import connect
>>> conn = connect(":memory:")  # createing in-memory database
>>> cursor = conn.cursor()  # getting the cursor
>>> cursor.execute("CREATE TABLE users (id, email)")  # create `users` table
>>> users = [
        (0, "George Torianik"),
        (1, "Vladimir Zelensky"),
        (2, "Glory to Ukraine"),
    ]
>>> cursor.executemany("INSERT INTO users VALUES (?,?)", users)  # inserting users to the table
>>> cursor.execute("SELECT * FROM users")
>>> assert cursor.fetchall() == users
```
