from typing import Any, Literal

from shared.sqlengine import SQLEngine
from shared.sqlengines._pandas import PandasSQLEngine
from shared.sqlengines._spark import SparkRedshiftEngine, SparkSQLEngine

SQLEngineNames = Literal["spark", "spark-redshift", "pandas"]


def get_sql_engine(
    name: SQLEngineNames,
    initargs: dict[str, Any] | None = None,
) -> SQLEngine:
    """Retrieves SQLEngine class from name.

    Parameters
    ----------
    name : str, {"spark", "spark-redshift", "pandas"}
        SQL engine to return.

    Returns
    --------
    sql_engine : SQLEngine
        SQLEngine instance.
    """
    # Return object if already is a SQLEngine instance.
    if isinstance(name, SQLEngine):
        return name

    if initargs is None:
        initargs = {}

    name_to_engine: dict[str, SQLEngine] = {
        "spark": SparkSQLEngine,
        "spark-redshift": SparkSQLEngine,
        "pandas": PandasSQLEngine,
    }

    if name not in name_to_engine:
        raise ValueError()

    return name_to_engine[name](**initargs)


__all__ = [
    "get_sql_engine",
    "SQLEngine",
    "PandasSQLEngine",
    "SparkSQLEngine",
    "SparkRedshiftEngine",
]
