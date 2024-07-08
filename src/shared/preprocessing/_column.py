import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.sql import Column, DataFrame
from sparkml_base_classes import TransformerBaseClass

from shared.typing import ColumnOrName, SQLFunction


class ColumnDropper(TransformerBaseClass):
    """Custom Transformer wrapper class for DataFrame.drop.

    From Docs:
    Returns a new DataFrame without specified columns.
    This is a no-op if the schema doesn't contain the given column name(s).

    Parameters
    ----------
    col : str or Column
        A name of the column, or the Column to drop.
    """

    @keyword_only
    def __init__(self, col: ColumnOrName = None):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:
        return X.drop(self._col)


class ColumnTransformer(TransformerBaseClass):
    """Constructs a transformer from an arbitrary callable.

    A ColumnTransformer forwards the column object from its input dataframe
    to a function object and returns the result of this function.

    Parameters
    ----------
    col : str or Column
        A name of the column, or the Column to transform.

    new_col : str or Column
        A name for the new transformed column. Choose this to be the same as
        ``col`` to overwrite its values.

    fn : SQLCallable
        Callable from pyspark.sql.functions

    kwargs : dict, default=None
        Kwargs to propagate to ``fn``.
    """

    @keyword_only
    def __init__(
        self,
        col: ColumnOrName = None,
        new_col: ColumnOrName = None,
        fn: SQLFunction = None,
        kwargs: dict | None = None,
    ):

        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:
        kwargs = {} if self._kwargs is None else self._kwargs
        Xt = X.withColumn(self.new_col, self._fn(F.col(self._col), **kwargs))
        return Xt


class ColumnSelector(TransformerBaseClass):
    """Custom Transformer wrapper class for DataFrame.select.

    Parameters
    ----------
    cols : list of str or Column
        Column names (string) or expressions (Column). If one of the column
        names is '*', that column is expanded to include all columns in the
        current DataFrame.
    """

    @keyword_only
    def __init__(self, cols: list[ColumnOrName] = None):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:
        return X.select(*self._cols)


class MultiColumnTransformer(TransformerBaseClass):
    """Applies multiple transformations to a single column.

    Each transformation corresponds to a new column in the returned DataFrame.

    Notes
    -----
    This transformers introduces multiple projections internally. Therefore,
    calling it with multiples functions to add multiple columns can generate
    big plans which can cause performance issues and even
    StackOverflowException. To avoid this, use :class:`ColumnSelector` with the
    multiple columns at once.

    """

    @keyword_only
    def __init__(
        self,
        col: ColumnOrName = None,
        fns: list[SQLFunction] = None,
        new_cols: list[ColumnOrName] = None,
    ):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:

        for fn, new_col in zip(self._fns, self._new_cols):
            ct = ColumnTransformer(fn=fn, col=self._col, new_col=new_col)
            X = ct.transform(X)

        return X


class DropDuplicates(TransformerBaseClass):
    """Custom Transformer wrapper class for DataFrame.dropDuplicates"""

    @keyword_only
    def __init__(self, subset: list[str] | None = None):
        super().__init__()

    def _transform(self, X: DataFrame) -> DataFrame:
        return X.dropDuplicates(self._subset)
