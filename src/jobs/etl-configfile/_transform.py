from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from sparkml_base_classes import TransformerBaseClass as SparkTransformer


def transform(
    X: DataFrame,
    stages: list[SparkTransformer],
) -> DataFrame:
    """Applies given stages to X one after another."""
    return Pipeline(stages=stages).fit(X).transform(X)
