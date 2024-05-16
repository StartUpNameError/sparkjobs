from pyspark.sql import DataFrame


class S3Loader:

    def __init__(self, path: str | None = None) -> None:
        self.path = path

    def load(self, ddf: DataFrame) -> None:
        ddf.write.parquet(self.path)
