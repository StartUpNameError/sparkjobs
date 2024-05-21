from dataclasses import dataclass
from typing import ClassVar, Protocol


class URLTemplate(Protocol):
    """Interface for URL formatters.

    Parameters
    ----------
    drivername: str
        The name of the database backend.

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
    """

    def format(
        self,
        drivername: str,
        host: str,
        port: str,
        username: str,
        password: str,
        database: str,
    ) -> str: ...


@dataclass(kw_only=True)
class URL:
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
    """

    DEFAULT_TEMPLATE: ClassVar = (
        "{drivername}://{username}:{password}@{host}:{port}/{database}"
    )

    drivername: str
    host: str
    port: str
    username: str
    password: str
    database: str

    def as_string(
        self,
        template: URLTemplate | None = None,
    ) -> str:
        """Creates URL string.

        Parameters
        ----------
        template : str, default=None
            Any object implementing the :meth:`format` method (e.g., a string) 
            with the following signature:

            def format(
                drivername: str,
                host: str,
                port: str,
                username: str,
                password: str,
                database: str
            ) -> str:
                ...

            If None, the typical form of a database URL is used which is,
            "{drivername}://{username}:{password}@{host}:{port}/{database}"


        Returns
        -------
        url : str
            URL rendered as string.
        """
        if template is None:
            template = self.DEFAULT_TEMPLATE

        return template.format(
            drivername=self.drivername,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )
