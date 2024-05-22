import abc
from typing import Any, Literal, Protocol

from shared.sqlengine import SQLEngine, SQLParams, SQLResponse
from shared.sqlengines import get_sql_engine
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

    Parameters
    ----------
    url_factory : URLFactory
        No args :class:`URL` factory.
    """

    def __init__(self, url_factory: URLFactory):
        self.url_factory = url_factory

    def execute(
        self,
        engine: Literal["spark", "pandas", "glue"] | SQLEngine = "spark",
        engine_config: dict[str, Any] | None = None,
    ) -> SQLResponse:
        """Executes query through specified engine.

        Parameters
        ----------
        engine : str or SQLEngine
            Engine for executing the query.

        engine_config : dict or None
            Engine configuration.
        """

        url = self.url_factory()
        query = self.create_sql()
        sql_engine = get_sql_engine(name=engine, initargs=engine_config)

        sql_params = SQLParams(url=url, query=query)
        return sql_engine.read_sql(sql_params=sql_params)

    @abc.abstractmethod
    def create_sql(self) -> str:
        """Returns sql query to be executed."""
        pass
