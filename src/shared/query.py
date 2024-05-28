import abc
from typing import Any, Callable, Protocol

from shared.sqlengine import SQLEngine
from shared.url import URL


class URLFactory(Protocol):
    """URL factory interface.

    In words, a URLFactory must be a no args callable that returns a URL object.
    """

    def __call__(self) -> URL:
        pass


class Query(abc.ABC):
    """Base class to inherit for concrete query executors.

    Concrete/derived Query classes must implement abstract method
    :meth:`create_query`.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def execute(
        self,
        engine: SQLEngine,
        **options,
    ) -> Any:
        """Executes query through specified engine.

        Parameters
        ----------
        engine : SQLEngine
            Engine for executing the query.

        **options : dict
            SQLEngine specific options.
        """
        sql = self.create_sql()
        return engine.load(sql, **options)

    @abc.abstractmethod
    def create_sql(self) -> str:
        """Returns sql query to be executed."""
        pass


class FunctionQuery(Query):
    """Constructs a query executor from an arbitrary callable.

    Parameters
    ----------
    func: callable
        The callable to use to create the sql string. This will be called with
        ``kw_args`` forwarded.

    kw_args : dict, default=None
        Dictionary of keyword arguments to pass to func.
    """

    def __init__(
        self,
        *,
        func: Callable[..., str],
        kw_args: dict | None = None,
    ) -> None:
        self.func = func
        self.kw_args = kw_args or {}

    def create_sql(self) -> str:
        return self.func(**self.kw_args)
