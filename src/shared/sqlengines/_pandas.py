import pandas as pd
from sqlalchemy.engine import create_engine

from shared.sqlengine import SQLEngine


class PandasSQLEngine(SQLEngine):
    """Read SQL query or database table into a pandas DataFrame.

    This function is a convenience wrapper around `pd.read_sql`.


    Parameters
    ----------
    url : str
        String connection url.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        super().__init__()

    def read_sql(self, sql) -> pd.DataFrame:
        con = create_engine(url=self.url)
        return pd.read_sql(sql=sql, con=con)
