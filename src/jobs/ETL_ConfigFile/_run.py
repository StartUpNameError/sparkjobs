import functools
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from sparkml_base_classes import TransformerBaseClass as SparkTransformer

from shared.configparser import ConfigParser
from shared.extractor import Extractor
from shared.loader import Loader
from shared.utils.spark import DataFrameMerger, intersection

from ._extract import extract
from ._transform import transform


def merge(
    dfs: dict[str, DataFrame],
    on: Callable[[DataFrame, DataFrame], list[str]] | list[str] | str,
    how: str = "inner",
) -> DataFrame:
    """Reduces dataframes into a single one containing all the features.

    Parameters
    ----------
    ddfs : list of spark DataFrame
        List of dataframes to reduce.

    on : callable or list of str or str
        A string for the join column name, a list of column names or a callable
        with the signature (x: DataFrame, y: DataFrame) -> list[str],
        that is, a function that takes the dataframes to be joined and returns
        the joining columns.

    how : str, default="inner"
        Join strategy.
    """
    merger = DataFrameMerger(on=on, how=how)
    return functools.reduce(merger, dfs)


def run(spark: SparkSession, config: str) -> None:

    parser: ConfigParser = ConfigParser.from_yaml(yamfile=config)

    # Parse objects from configfile.
    extractors: list[Extractor] = parser.parse("extractors").values()
    stages: list[SparkTransformer] = parser.parse("stages").values()
    loader: Loader = parser.parse("loader").values()

    extraction = extract(extractors=extractors)
    ddf = merge(dfs=extraction, on=intersection)
    X = transform(ddf=ddf, stages=stages)
    loader.load(X)
