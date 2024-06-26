from __future__ import annotations

import abc
from typing import Callable

from sqlengine import SQLEngine
from sqlengines import PandasSQLEngine

from shared.extractor import DataFrame
from shared.url import URL


class SQLExtractor(abc.ABC):
    """Base class to inherit for concrete SQLExtractor's.

    Concrete/derived SQLExtractor classes must implement abstract methods
    :meth:`create_url` and :meth:`create_sql`.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self) -> None:
        self._engine: SQLEngine = PandasSQLEngine()

    @property
    def engine(self) -> SQLEngine:
        return self._engine

    @engine.setter
    def engine(self, engine: SQLEngine) -> None:
        self._engine = engine

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
        url_factory: Callable[..., URL],
        kw_args: dict | None = None,
    ) -> None:
        self.func = func
        self.url_factory = url_factory
        self.kw_args = kw_args or {}

    def create_sql(self) -> str:
        return self.func(**self.kw_args)
