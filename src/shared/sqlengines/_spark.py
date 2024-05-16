from pyspark.sql import DataFrameReader, SparkSession

from shared.settings import SparkSettings, conf
from shared.sqlengine import SQLEngine, SQLResponse

settings: SparkSettings = conf.get_spark_settings()


class SparkSQLEngine(SQLEngine):
    """Lets you query structured data inside Spark programs."""

    def read_sql(self, sql: str) -> SQLResponse:

        spark: SparkSession = SparkSession.builder.getOrCreate()
        db_engine = self.connection.get_db_engine()

        options = {
            "url": self.connection.url(),
            "tempdir": settings.tempdir,
            "query": sql,
            **db_engine.spark_options,
        }

        format_ = db_engine.spark_format
        reader: DataFrameReader = spark.read.format(format_).options(**options)
        return SQLResponse(dataframe=reader.load())
