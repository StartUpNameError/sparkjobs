import pandas as pd
from sqlalchemy.engine import create_engine

from shared.sqlengine import SQLEngine, SQLResponse
from shared.url import URL


class PandasSQLEngine(SQLEngine):
    """Wrapper around `pd.read_sql`."""

    def __init__(self, url: URL):
        super().__init__(url)

    def read_sql(self, sql: str) -> SQLResponse:

        con = create_engine(url=self.url.as_string())
        dataframe = pd.read_sql(sql=sql, con=con)
        return SQLResponse(dataframe=dataframe)
