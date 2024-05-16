from __future__ import annotations

import abc
from typing import Literal, Protocol

from shared.connection import Connection
from shared.sqlengine import SQLEngine, SQLResponse
from shared.sqlengines import get_sql_engine


class ConnectionFactory(Protocol):
    """Connection factory interface.

    Any ConnectionFactory must be a no args callable that returns a Connection
    object.
    """

    def __call__(self) -> Connection:
        pass


class Query(abc.ABC):
    """Base class to inherit for concrete query executors.

    Concrete/derived Query classes must implement abstract method
    :meth:`create_query`.

    . note::
        This class should not be used directly. Use derived classes instead.

    Parameters
    ----------
    connection_factory : ConnectionFactory
        No args :class:`Connection` factory.
    """

    def __init__(self, connection_factory: ConnectionFactory):
        self.connection_factory = connection_factory

    def execute(
        self,
        engine: Literal["spark", "pandas", "glue"] = "spark",
    ) -> SQLResponse:
        sql_engine = get_sql_engine(engine)
        sql_engine: SQLEngine = sql_engine(connection=self.connection_factory())
        return sql_engine.read_sql(sql=self.create_sql())

    @abc.abstractmethod
    def create_sql(self) -> str:
        """Returns sql query to be executed."""
        pass
