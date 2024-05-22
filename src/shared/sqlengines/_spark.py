from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from shared.settings import conf
from shared.sqlengine import SQLEngine, SQLParams, SQLResponse

settings = conf.get_spark_settings()
jdbcTemplate = "{drivername}://{host}:{port}/{database}?user={username}&password={password}"


class SparkSQLEngine(SQLEngine):
    """Lets you query structured data inside Spark programs.

    Parameters
    ----------
    format : str, default="jdbc"
        String for format of the data source. Default to 'jdbc'.

     schema : :class:`pyspark.sql.types.StructType` or str, default=None
        Optional :class:`pyspark.sql.types.StructType` for the input schema
        or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

    options : dict, default=None
        All other string options.
    """

    def __init__(
        self,
        format: str = "jdbc",
        schema: StructType | str | None = None,
        options: dict[str, str] = None,
    ):
        self.format = format
        self.schema = schema
        self.options = options or {}

    def read_sql(self, sql_params: SQLParams) -> SQLResponse:

        spark = SparkSession.builder.getOrCreate()

        options = {
            "url": sql_params.url.as_string(template=jdbcTemplate),
            "tempdir": settings.tempdir,
            "query": sql_params.query,
            **self.options,
        }

        dataframe: DataFrame = spark.read.load(
            format=self.format,
            schema=self.schema,
            options=options,
        )

        return SQLResponse(dataframe=dataframe)
