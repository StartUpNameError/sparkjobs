from __future__ import annotations

import os
from dataclasses import dataclass

from shared.definitions import DATA_DIR
from shared.loaders import JSONFileLoader

__all__ = ["DBEngine", "get_db_engine"]


@dataclass(kw_only=True)
class DBEngine:
    """Container for db engine related attributes.

    Each database engine (e.g., redshift, postgres, mysql) has different drivernames
    depending on the query executor (e.g., sqlalchemy, spark) and possibly 
    different spark options.
    """

    drivers: dict[str, str]
    spark: dict[str, str]

    def get_drivername(self, name: str) -> str | None:
        return self.drivers.get(name)

    @property
    def spark_format(self) -> str | None:
        return self.spark.get("format")

    @property
    def spark_options(self) -> dict | None:
        return self.spark.get("options")


def create_db_engines() -> dict[str, DBEngine]:

    db_engines: dict[str, DBEngine] = {}

    db_engines_path: str = os.path.join(DATA_DIR, "dbengines")
    db_engines_data: list[dict] = JSONFileLoader().load(db_engines_path)

    for data in db_engines_data:
        name = data.pop("name")
        db_engines[name] = DBEngine(**data)

    return db_engines


db_engines = create_db_engines()


def get_db_engine(name: str) -> DBEngine:
    """Retrieves database engine from name.

    Raises
    ------
    KeyError if database engine does not exist.

    Parameters
    ----------
    name : str
        Name of the db engine.

    Returns
    -------
    db_engine : DBEngine
    """
    if name not in db_engines:
        raise

    return db_engines[name]
