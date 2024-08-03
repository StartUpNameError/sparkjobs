from typing import Any

from shared.sqlengine import SQLEngine
from shared.sqlengines._pandas import PandasSQLEngine
from shared.sqlengines._spark import SparkSQLEngine

__all__ = ["get_sql_engine", "SQLEngine"]


def get_sql_engine(
    name: str,
    initargs: dict[str, Any] | None = None,
) -> SQLEngine:
    """Retrieves SQLEngine instance from name.

    Parameters
    ----------
    name : str, {"spark", "pandas"}
        SQL engine to return.

    initargs: dict or None, default=None
        Arguments used to construct instance.

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
        "pandas": PandasSQLEngine,
    }

    if name not in name_to_engine:
        raise ValueError(f"SQLEngine `{name}` not found.")

    return name_to_engine[name](**initargs)
