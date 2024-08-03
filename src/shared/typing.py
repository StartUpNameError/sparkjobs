"""
Variables defined here are used across the library for type hint.
"""

from typing import Annotated, Callable, Literal

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame

ColumnOrName = Column | str

DataFrame = SparkDataFrame | PandasDataFrame

SQLFunction = Annotated[Callable, "Callable from pyspark.sql.functions"]

DatetimeAttribute = Literal["dayofweek", "month"]

OnFunction = Callable[[DataFrame, DataFrame], list[str] | str]
