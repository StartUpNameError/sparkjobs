import abc
from dataclasses import dataclass

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame

from shared.connection import Connection

DataFrame = PandasDataFrame | SparkDataFrame


@dataclass(kw_only=True)
class SQLResponse:
    dataframe: DataFrame


class SQLEngine(abc.ABC):
    """Base class to inherit for concrete SQLEngine's.

    Concrete/derived SQLEngine classes must implement abstract method
    :meth:`read_sql`.

     . note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self, connection: Connection):
        self.connection = connection

    @abc.abstractmethod
    def read_sql(self, sql: str) -> SQLResponse:
        pass
