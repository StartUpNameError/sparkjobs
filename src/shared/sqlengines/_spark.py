from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from shared.settings import conf
from shared.sqlengine import SQLEngine
from shared.url import URL

settings = conf.get_spark_settings()

JDBC_TEMPLATE: str = "{drivername}://{host}:{port}/{database}?user={username}&password={password}"


class SparkSQLEngine(SQLEngine):
    """Lets you query structured data inside Spark programs.

    Parameters
    ----------
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
        format: str = "jdbc",
        schema: StructType | str | None = None,
        spark_options: dict[str, str] = None,
    ):
        self.format = format
        self.schema = schema
        self.spark_options = spark_options or {}

        super().__init__()

    def read_sql(self, sql: str, url: URL) -> DataFrame:

        spark = SparkSession.builder.getOrCreate()

        options = {
            "tempdir": settings.tempdir,
            "query": sql,
            "url": url.as_string(template=JDBC_TEMPLATE),
            **self.spark_options,
            **self._options,
        }

        dataframe: DataFrame = spark.read.load(
            format=self.format,
            schema=self.schema,
            **options,
        )

        return dataframe
