from concurrent.futures import Executor, Future, ThreadPoolExecutor
from typing import Any, Protocol


class Executable(Protocol):

    def execute(self) -> Any: ...


class ParallelExecutor:
    def __init__(
        self,
        executables: list[Executable],
        executor: Executor = ThreadPoolExecutor(),
    ) -> None:
        self.executables = executables
        self.executor = executor

    def execute(self):
        futures: list[Future] = []

        for executable in self.executables:
            future = self.executor.submit(executable.execute)
            futures.append(future)

        return futures
