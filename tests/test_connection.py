from shared.connection import Connection


def test_postgres_connection_from_secret():
    secret = {
        "host": "host",
        "port": 5432,
        "username": "username",
        "password": "password",
        "dbname": "dbname",
        "engine": "postgres",
    }

    conn = Connection.from_secret(secret)

    url = str(conn.url(driver="jdbc"))
    assert url == "jdbc:postgresql://username:password@host:5432/dbname"

    url = str(conn.url(driver="alchemy"))
    assert url == "postgresql://username:password@host:5432/dbname"
