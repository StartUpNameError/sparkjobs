"""
Prepares data for ML forecasting algorithms.

The scripts is composed of 3 stages.

1. Extract: Collect the results of multiples queries and merge them to create 
a single dataframe. That is, once we run the queries, we can read each result 
into a dataframe and then merge them all on the primary key to give a single 
resultant data source having all the features.

2. Transform (Feature extraction and selection): The data prepared in the 
previous stage is ready to be transformed into more meaningful features.
This stage is usually more computation intensive since feature 
extraction/selection proccesses rely on numerical algorithms.

3. Load: Load transformed data to its next destination.
"""

import functools
from concurrent.futures import wait
from typing import Callable

import click
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from sparkml_base_classes import TransformerBaseClass as SparkTransformer

from shared.concurrent import ParallelExecutor
from shared.configparser import ConfigParser
from shared.loader import Loader
from shared.preprocessing import DateNormalizer
from shared.query import Query
from shared.utils.spark import DataFrameJoin, intersection


def extract(queries: list[Query]) -> list[DataFrame]:
    """Executes queries concurrently and waits for results.

    Parameters
    ----------
    queries : list of Query
        Queries to execute.

    Returns
    -------
    ddfs : list of spark DataFrame
    """
    results = wait(ParallelExecutor(executables=queries).execute())
    ddfs = [f.result().dataframe for f in results.done]
    return ddfs


def join(
    ddfs: list[DataFrame],
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
    normalizer = DateNormalizer(col="date", format_="yyyy-MM-dd")
    ddfs = list(map(normalizer.transform, ddfs))
    join = DataFrameJoin(on=on, how=how)
    return functools.reduce(join, ddfs)


def transform(
    ddf: DataFrame,
    stages: list[SparkTransformer],
) -> DataFrame:
    """Applies given stages to dataframe, one after another."""
    return Pipeline(stages=stages).fit(ddf).transform(ddf)


@click.option(
    "--config",
    required=True,
    type=str,
)
@click.option(
    "--request_id",
    required=True,
    type=int,
)
def main(config: str, request_id: int) -> None:

    parser: ConfigParser = ConfigParser.from_yaml(
        yamfile=config, context=context
    )

    # Parse objects from configfile.
    queries: list[Query] = parser.parse("queries").values()
    stages: list[SparkTransformer] = parser.parse("stages").values()
    loader: Loader = parser.parse("loader").values()

    # ETL.
    ddfs = extract(queries=queries)
    ddf = join(ddfs=ddfs, on=intersection, how="inner")
    ddft = transform(ddf=ddf, stages=stages)
    loader.load(ddf=ddft)

    print("-- FINISH ETL ---")


if __name__ == "__main__":
    main()
