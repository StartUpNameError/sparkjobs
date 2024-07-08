from pyspark.sql import DataFrame


class ParquetLoader:
    """Saves the content of the :class:`DataFrame` in Parquet format at the 
    specified path.

    Parameters
    ----------
    path : str
        the path in any Hadoop supported file system
    
    mode : str, optional
        specifies the behavior of the save operation when data already exists.

        * ``append``: Append contents of this :class:`DataFrame` to existing data.
        * ``overwrite``: Overwrite existing data.
        * ``ignore``: Silently ignore this operation if data already exists.
        * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
            exists.
    
    partitionBy : str or list, optional
        names of partitioning columns
    """

    def __init__(
        self,
        path: str | None = None,
        mode: str | None = None,
        partitionBy: str | list[str] | None = None,
        compression: str | None = None,
    ) -> None:
        self.path = path
        self.mode = mode
        self.partitionBy = partitionBy
        self.compression = compression

    def load(self, X: DataFrame) -> None:
        X.write.parquet(
            self.path,
            mode=self.mode,
            partitionBy=self.partitionBy,
            compression=self.compression,
        )
