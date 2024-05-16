from typing import Callable

from shared.query import ConnectionFactory, Query


class FunctionQuery(Query):
    """Constructs a query executor from an arbitrary callable.

    Parameters
    ----------
    func: callable
        The callable to use to create the sql string. This will be called the
        with kwargs forwarded.

    connection_factory : ConnectionFactory
        No args connection factory.

    kw_args : dict, default=None
        Dictionary of keyword arguments to pass to func.
    """

    def __init__(
        self,
        *,
        func: Callable[..., str],
        connection_factory: ConnectionFactory,
        kw_args: dict | None = None,
    ) -> None:
        self.func = func
        self.kw_args = kw_args or {}

        super().__init__(connection_factory=connection_factory)

    def create_sql(self) -> str:
        return self.func(**self.kw_args)
