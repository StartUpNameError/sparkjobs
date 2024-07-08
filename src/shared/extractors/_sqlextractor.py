from __future__ import annotations

import abc
from typing import Callable

import pypika

from shared.extractor import DataFrame
from shared.sqlengine import SQLEngine
from shared.sqlengines import SparkSQLEngine, get_sql_engine
from shared.url import URL


class SQLExtractor(abc.ABC):
    """Base class to inherit for concrete SQLExtractor's.

    Concrete/derived SQLExtractor classes must implement abstract methods
    :meth:`create_url` and :meth:`create_sql`.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self) -> None:
        self._engine: SQLEngine = SparkSQLEngine()

    @property
    def builder(self) -> pypika.Query:
        return pypika.Query

    @property
    def engine(self) -> SQLEngine:
        return self._engine

    @engine.setter
    def engine(self, engine: SQLEngine) -> None:
        self._engine = engine

    def set_engine(self, engine: SQLEngine) -> SQLExtractor:
        """Sets a new SQLEngine and returns self"""
        self.engine = get_sql_engine(engine)
        return self

    def extract(self) -> DataFrame:
        """Executes query through specified engine.

        Parameters
        ----------
        engine : SQLEngine
            Engine for executing the query.

        **options : dict
            SQLEngine specific options.
        """
        return self.engine.read_sql(sql=self.sql(), url=self.url())

    def table(self, name: str) -> pypika.Table:
        return pypika.Table(name)

    @abc.abstractmethod
    def url(self) -> URL:
        """Returns databse connection URL."""
        pass

    @abc.abstractmethod
    def sql(self) -> str:
        """Returns sql query to be executed."""
        pass


class FunctionSQLExtractor(SQLExtractor):
    """Constructs a SQLExtractor from an arbitrary callable.

    Parameters
    ----------
    func: callable
        The callable to use to create the sql string. This will be called with
        ``kw_args`` forwarded.

    kw_args : dict, default=None
        Dictionary of keyword arguments to pass to ``func``.
    """

    def __init__(
        self,
        *,
        func: Callable[..., str],
        url_factory: Callable[[], URL],
        kw_args: dict | None = None,
    ) -> None:
        self.func = func
        self.url_factory = url_factory
        self.kw_args = kw_args or {}

        super().__init__()

    def sql(self) -> str:
        return self.func(**self.kw_args)

    def url(self) -> URL:
        return self.url_factory()


class SecretSQLExtractor(SQLExtractor):

    def __init__(self, secret_id: str) -> None:
        self.secret_id = secret_id
        
        super().__init__()

    @abc.abstractmethod
    def sql(self) -> str:
        """Returns sql query to be executed."""
        pass

    def url(self) -> URL:
        return URL.from_secret(secret_id=self.secret_id)
