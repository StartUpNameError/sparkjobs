from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from shared.settings import conf
from shared.sqlengine import SQLEngine
from shared.url import URL

settings = conf.get_spark_settings()

JDBC_TEMPLATE: str = "jdbc:{drivername}://{host}:{port}/{database}"


class SparkSQLEngine(SQLEngine):
    """Lets you query structured data inside Spark programs.

    Parameters
    ----------
    format : str, default="jdbc"
        String for format of the data source. Default to 'jdbc'.

    schema : :class:`pyspark.sql.types.StructType` or str, default=None
        Optional :class:`pyspark.sql.types.StructType` for the input schema
        or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

    options : key-word args
        Spark options.
    """

    def __init__(
        self,
        format: str = "jdbc",
        schema: StructType | str | None = None,
        **options
    ):
        self.format = format
        self.schema = schema
        self.options = options

        super().__init__()

    def read_sql(self, sql: str, url: URL) -> DataFrame:

        spark = SparkSession.builder.getOrCreate()

        options = {
            "query": sql,
            "url": url.as_string(template=JDBC_TEMPLATE),
            "user": url.username,
            "password": url.password,
        }

        options.update(self.options)
        dataframe: DataFrame = spark.read.load(
            format=self.format,
            schema=self.schema,
            **options,
        )

        return dataframe


class SparkRedshiftEngine(SparkSQLEngine):
    """Concrete SparkSQLEngine for AWS Redshift

    Parameters
    ----------
    options : key-word args
        Spark options.
    """

    def __init__(**options):

        super().__init__(
            format="io.github.spark_redshift_community.spark.redshift",
            tempdir=settings.tempdir,
            forward_spark_s3_credentials="true",
            **options,
        )
