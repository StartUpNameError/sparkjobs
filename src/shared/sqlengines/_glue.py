from shared.sqlengine import SQLEngine


class GlueSQLEngine(SQLEngine):

    def read_sql(self, sql: str, **kwargs):
        return super().read_sql(sql, **kwargs)
