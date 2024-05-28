from __future__ import annotations

import abc
from typing import Any, Type

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame

DataFrame = PandasDataFrame | SparkDataFrame


class MissingOption(KeyError):
    def __init__(self, option: str) -> None:
        self.option = option

    def __repr__(self):
        msg = """\
        Missing option `{0}`. Please set this option through the `option`
        method or as keyword argument in `load`.\
        """

        return msg.format(self.option)


def to_str(value: Any) -> str:
    """A wrapper over str(), but converts bool values to lower case strings.

    If None is given, just returns None, instead of converting it to string "None".
    """
    if isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return value
    else:
        return str(value)


class AttributeHolder(dict):
    """Map of registered options."""

    def __init__(self, exc: Type[Exception] = KeyError):
        self.exc = exc

    def __missing__(self, key: str):
        raise self.exc(key)

    def __getattr__(self, attr: str):
        return self[attr]


class SQLEngine(abc.ABC):
    """Base class to inherit for concrete SQLEngine's.

    Concrete/derived SQLEngine classes must implement abstract method
    :meth:`read_sql`.

     . note::
        This class should not be used directly. Use derived classes instead.

    Parameters
    ----------
    url : URL
        Represents the components of a URL used to connect to a database.
    """

    def __init__(self) -> None:
        self._options = AttributeHolder(exc=MissingOption)

    def load(self, sql: str, **options) -> Any:
        """Executes SQL query.

        Parameters
        ----------
        sql: str
            SQL query to be executed
        """
        self.options(**options)
        return self.read_sql(sql)

    def options(self, **options) -> SQLEngine:
        for key, val in options.items():
            self.option(key, to_str(val))

        return self

    def option(self, key: str, value: Any) -> SQLEngine:
        self._options[key] = value
        return self

    @abc.abstractmethod
    def read_sql(self, sql: str) -> DataFrame:
        pass
