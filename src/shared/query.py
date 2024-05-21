import abc
from typing import Literal, Protocol

from shared.sqlengine import SQLEngine, SQLResponse
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
        engine: Literal["spark", "pandas", "glue"] = "spark",
    ) -> SQLResponse:

        sql_engine: SQLEngine = get_sql_engine(engine)(url=self.url_factory())
        return sql_engine.read_sql(sql=self.create_sql())

    @abc.abstractmethod
    def create_sql(self) -> str:
        """Returns sql query to be executed."""
        pass
