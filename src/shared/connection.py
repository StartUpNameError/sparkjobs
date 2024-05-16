from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

import sqlalchemy.engine

from shared.dbengine import DBEngine, get_db_engine
from shared.urls import URL


class SecretDict(TypedDict):
    host: str
    port: int
    dbname: str
    engine: str
    username: str
    password: str
    dbClusterIdentifier: str


class SQLAlchemyURL(URL):
    """SQLAlchemy URL creator.

    SQLAlchemy provides the URL.create() constructor method which produces the
    typical form of a database URL, which is:

    dialect+driver://username:password@host:port/database
    """

    def __init__(self) -> None:
        super().__init__(template=sqlalchemy.engine.URL.create)


@dataclass(kw_only=True)
class Connection:
    """Represents the components of a URL used to connect to a database.

    Parameters
    ----------
    host : str
        The name of the host.

    port : int
        The port number.

    username : str
        The user name.

    password : str
        Database password. Is typically a string, but may also be an object that
        can be stringified with str().

    database : str
        The database name.

    engine : str
        The name of the database backend.
    """

    host: str
    port: str
    username: str
    password: str
    database: str
    engine: str

    def url(
        self,
        driver: str = "jdbc",
    ) -> str:
        """Creates URL string.

        Parameters
        ----------
        driver : str, default="jdbc"
            Driver to use for constructing the url.

        template : URL
            

        Returns
        -------
        url : str
            URL rendered as string.
        """
        drivername = self.get_db_engine().get_driver(driver)
        template = get_template(driver)

        return template.format(
            drivername=drivername,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

    def make_alchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Returns instance of :class:`sqlalchemy.engine.Engine`.

        Once created, can either be used directly to interact with the database,
        or can be passed to a Session object to work with the ORM.

        Returns
        -------
        sqlalchemy.engine.Engine
        """
        url = self.url(driver="alchemy")
        return sqlalchemy.engine.create_engine(url=url)

    def get_db_engine(self) -> DBEngine:
        """Returns db engine.

        Returns
        -------
        db_engine : DBEngine
        """
        return get_db_engine(self.engine)

    @classmethod
    def from_secret(cls, secret: SecretDict) -> Connection:
        """Creates Connection object from secret."""

        return cls(
            host=secret["host"],
            port=secret["port"],
            username=secret["username"],
            password=secret["password"],
            database=secret["dbname"],
            engine=secret["engine"],
        )
