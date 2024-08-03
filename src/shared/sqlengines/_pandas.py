import pandas as pd

from shared.sqlengine import SQLEngine
from shared.url import URL


class PandasSQLEngine(SQLEngine):
    """Read SQL query or database table into a pandas DataFrame.

    This function is a convenience wrapper around `pd.read_sql`.
    """

    def __init__(self) -> None:
        super().__init__()

    def read_sql(self, sql: str, url: URL) -> pd.DataFrame:
        return pd.read_sql(sql=sql, con=url.as_string())
