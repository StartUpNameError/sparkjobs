from typing import Protocol

from shared.typing import DataFrame


class Extractor(Protocol):
    """Extractor interface.
    
    
    Extracts are objects implementing the :class:`Extractor` interface. 
    That is, method :meth:`extract` must be implemented. For example, 
    the following class is a convenience wrapper around pandas to allows 
    to satisfy the :class:`Extractor` interface:

    ```
    import pandas as pd

    class PandasSQLExtractor:
        def __init__(self, sql, con):
            self.sql=sql
            self.con=con

        def extract(self) -> DataFrame
            return pd.read_sql(sql=sql, con=con)
    ```
    """

    def extract(self) -> DataFrame:
        """Extracts data from arbitrary data source."""
        ...
