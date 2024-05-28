from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from shared.settings import conf
from shared.sqlengine import SQLEngine, SQLParams, SQLResponse
from shared.url import URL

settings = conf.get_spark_settings()


class SparkSQLEngine(SQLEngine):
    """Lets you query structured data inside Spark programs.

    Parameters
    ----------
    url : str
        String connection.

    format : str, default="jdbc"
        String for format of the data source. Default to 'jdbc'.

    schema : :class:`pyspark.sql.types.StructType` or str, default=None
        Optional :class:`pyspark.sql.types.StructType` for the input schema
        or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

    spark_options : dict, default=None
        All other spark options.
    """

    def __init__(
        self,
        url: str,
        format: str = "jdbc",
        schema: StructType | str | None = None,
        spark_options: dict[str, str] = None,
    ):
        self.url = url
        self.format = format
        self.schema = schema
        self.spark_options = spark_options or {}

        super().__init__()

    def read_sql(self, sql: str) -> DataFrame:

        spark = SparkSession.builder.getOrCreate()

        options = {
            "tempdir": settings.tempdir,
            "query": sql,
            "url": self.url,
            **self.spark_options,
        }

        dataframe: DataFrame = spark.read.load(
            format=self.format,
            schema=self.schema,
            options=options,
        )

        return dataframe
