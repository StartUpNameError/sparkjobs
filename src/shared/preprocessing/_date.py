import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.ml import Pipeline
from sparkml_base_classes import TransformerBaseClass

from ._column import ColumnTransformer


class DateNormalizer(TransformerBaseClass):
    """Custom Transformer wrapper class for functions.date_format().

    From Docs:
    Converts a date/timestamp/string to a value of string in the format
    specified by the date format given by the second argument.

    Parameters
    ----------
    col : str
        Column name to cast to date type.

    format : str
        The strftime to parse time, e.g. "%d/%m/%Y".
    """

    @keyword_only
    def __init__(self, col=None, format_=None):
        super().__init__()

    def _transform(self, ddf):
        date_col = F.to_date(F.col(self._col), self._format_)
        return ddf.withColumn(self._col, date_col)


class DateSplitter(TransformerBaseClass):

    @keyword_only
    def __init__(
        self, datecol: str = "date", dateattrs: tuple = ("dayofweek", "month")
    ):
        super().__init__()

    def _transform(self, ddf):

        fns = {"dayofweek": F.dayofweek, "month": F.month}

        for attr in self._dateattrs:
            if attr not in fns:
                raise ValueError(
                    f"Date attribute '{attr}' is not valid. "
                    f"Availables are {list(fns)}."
                )

        stages = [
            ColumnTransformer(col=self._datecol, newcol=attr, fn=fns[attr])
            for attr in self._dateattrs
        ]
        return Pipeline(stages=stages).fit(ddf).transform(ddf)
