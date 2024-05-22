import pandas as pd
from sqlalchemy.engine import create_engine

from shared.sqlengine import SQLEngine, SQLParams, SQLResponse


class PandasSQLEngine(SQLEngine):
    """Wrapper around `pd.read_sql`."""

    def read_sql(self, sql_params: SQLParams) -> SQLResponse:

        con = create_engine(url=sql_params.url.as_string())
        dataframe = pd.read_sql(sql=sql_params.query, con=con)
        return SQLResponse(dataframe=dataframe)
