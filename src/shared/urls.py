import abc
from typing import Protocol

from sqlalchemy.engine import URL


class URLFormatter(Protocol):

    def __call__(
        self,
        drivername: str,
        host: str,
        port: str,
        username: str,
        password: str,
        database: str,
    ) -> str: ...


class URLTemplate(abc.ABC):

    def __init__(self, formatter: URLFormatter) -> None:
        self.formatter = formatter

    def format(
        self,
        drivername: str,
        host: str,
        port: str,
        username: str,
        password: str,
        database: str,
    ) -> str:
        return self.formatter(
            drivername=drivername,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )


class SQLAlchemyTemplate(URLTemplate):

    def __init__(self) -> None:
        super().__init__(template=URL.create)


class JDBCTemplate(URLTemplate):

    def __init__(self) -> None:
        string = "{drivername}://{host}:{port}/{database}?user={username}&password={password}"
        super().__init__(template=string.format)
