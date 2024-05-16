from typing import Literal

from shared.sqlengine import SQLEngine
from shared.sqlengines._glue import GlueSQLEngine
from shared.sqlengines._pandas import PandasSQLEngine
from shared.sqlengines._spark import SparkSQLEngine


def get_sql_engine(name: Literal["spark", "pandas", "glue"]) -> SQLEngine:
    """Retrieves SQLEngine class from name.

    Parameters
    ----------
    name : str, {"spark", "pandas", "glue"}
        SQL engine to return.
    """
    name_to_engine: dict[str, SQLEngine] = {
        "spark": SparkSQLEngine,
        "pandas": PandasSQLEngine,
        "glue": GlueSQLEngine,
    }

    if name not in name_to_engine:
        raise ValueError()

    return name_to_engine[name]


__all__ = [
    "get_sql_engine",
    "PandasSQLEngine",
    "SparkSQLEngine",
    "GlueSQLEngine",
]
