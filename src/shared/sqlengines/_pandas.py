import pandas as pd

from shared.sqlengine import SQLEngine, SQLResponse


class PandasSQLEngine(SQLEngine):

    def read_sql(self, sql: str) -> SQLResponse:
        con = self.connection.create_alchemy()
        dataframe = pd.read_sql(sql=sql, con=con)
        return SQLResponse(dataframe=dataframe)
