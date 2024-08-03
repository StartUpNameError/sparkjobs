from __future__ import annotations

import abc
from typing import Callable

from shared.extractor import DataFrame
from shared.sqlengine import SQLEngine
from shared.sqlengines import SparkSQLEngine, get_sql_engine
from shared.url import URL


class SQLExtractor(abc.ABC):
    """Base class to inherit for concrete SQLExtractor's.

    Concrete/derived SQLExtractor classes must implement abstract method
    :meth:`sql`.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self) -> None:
        self._engine: SQLEngine = SparkSQLEngine()
        self._url: URL | None = None

    @property
    def engine(self) -> SQLEngine:
        return self._engine

    @property
    def url(self) -> SQLEngine:
        return self._url

    @engine.setter
    def engine(self, engine: SQLEngine) -> None:
        self._engine = get_sql_engine(engine)

    @engine.setter
    def url(self, url: URL) -> None:
        self._url = url

    def extract(self) -> DataFrame:
        """Executes query through specified engine.

        Parameters
        ----------
        engine : SQLEngine
            Engine for executing the query.

        **options : dict
            SQLEngine specific options.
        """
        if self.url is None:
            raise ValueError()

        return self.engine.read_sql(sql=self.sql(), url=self.url)

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
        kw_args: dict | None = None,
    ) -> None:
        self.func = func
        self.kw_args = kw_args or {}

        super().__init__()

    def sql(self) -> str:
        return self.func(**self.kw_args)
