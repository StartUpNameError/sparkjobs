from typing import Protocol

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame

DataFrame = PandasDataFrame | SparkDataFrame


class Extractor(Protocol):
    """Extractor interface."""

    def extract(self) -> DataFrame: ...
