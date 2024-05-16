from typing import Protocol

from pyspark.sql import DataFrame


class Loader(Protocol):

    def load(self, ddf: DataFrame): ...
