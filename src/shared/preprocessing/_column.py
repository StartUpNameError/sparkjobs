import pyspark.sql.functions as F
from pyspark import keyword_only
from sparkml_base_classes import TransformerBaseClass


class ColumnDropper(TransformerBaseClass):
    """Custom Transformer wrapper class for DataFrame.drop.

    From Docs:
    Returns a new DataFrame without specified columns.
    This is a no-op if the schema doesn't contain the given column name(s).

    Parameters
    ----------
    """

    @keyword_only
    def __init__(self, col=None):
        super().__init__()

    def _transform(self, ddf):
        return ddf.drop(self._col)


class ColumnTransformer(TransformerBaseClass):
    """Constructs a transformer from an arbitrary callable.

    A ColumnTransformer forwards the column object from its input dataframe
    to a user-defined function or function object and returns the result of
    this function.

    Note: If a lambda is used as the function, then the resulting transformer
    will not be pickleable.

    Parameters
    ----------
    """

    @keyword_only
    def __init__(
        self,
        col: str = None,
        newcol: str = None,
        fn: callable = None,
        kwargs: dict | None = None,
        drop: bool = False,
    ):

        super().__init__()

    def _transform(self, ddf):
        kwargs = {} if self._kwargs is None else self._kwargs
        col = self._col if self._newcol is None else self._newcol
        ddft = ddf.withColumn(col, self._fn(F.col(self._col), **kwargs))

        if self._drop and self._newcol is not None:
            return ddft.drop(self._col)

        return ddft
