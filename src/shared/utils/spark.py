from typing import Callable

from pyspark.sql import DataFrame


def intersection(x: DataFrame, y: DataFrame) -> list[str]:
    """Returns columns intersection between x and y.

    Parameters
    ----------
    x : DataFrame
        Spark DataFrame

    y : DataFrame
        Spark DataFrame

    Returns
    -------
    intersection : list[str]
        Columns intersection between x and y.
    """
    return list(set(x.columns).intersection(y.columns))


class DataFrameJoin:
    """Wraps DataFrame.join operation.

    Parameters
    ----------
    on : callable or list of str or str
        A string for the join column name, a list of column names or a callable
        with the signature (x: DataFrame, y: DataFrame) -> list[str],
        that is, a function that takes the dataframes to be joined and returns
        the joining columns.

    how : str, default="inner"
        Join strategy.
    """

    def __init__(
        self,
        on: Callable[[DataFrame, DataFrame], list[str]] | list[str],
        how: str = "inner",
    ) -> None:
        self.on = on
        self.how = how

    def __call__(self, left: DataFrame, right: DataFrame) -> Any:
        """Joins left and right dataframes.

        left : DataFrame
            Left dataframe.

        right : DataFrame
            Right dataframe

        """
        on = self.on(left, right) if callable(self.on) else self.on
        return left.join(right, on=on, how=self.how)
