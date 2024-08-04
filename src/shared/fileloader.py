import os
from typing import Any, Callable


class FileLoader:

    def __init__(self, loader: Callable, ext: str, open_method: Callable = open):
        self.loader = loader
        self.ext = ext
        self.open_method = open_method

    def exists(self, file_path: str) -> bool:
        """Checks if the file exists.

        Parameters
        ----------
        file_path: str
            The full path to the file to load without the extension.

        Returns
        -------
        True if file path exists, False otherwise.
        """
        if os.path.isfile(file_path + self.ext):
            return True
        return False

    def _load_file(self, file_path: str, kw_args: dict | None) -> Any:
        if not self.exists(file_path):
            return

        if kw_args is None:
            kw_args = {}

        # By default the file will be opened with locale encoding on Python 3.
        # We specify "utf8" here to ensure the correct behavior.
        full_path = file_path + self.ext
        print(f'Full path: {full_path}')
        with self.open_method(full_path, "rb") as fp:
            payload = fp.read().decode("utf-8")

        return self.loader(payload, **kw_args)

    def load(self, file_path: str, kw_args: dict | None = None) -> Any | None:
        """Attempt to load the file path.

        Parameters
        ----------
        file_path: str
            The full path to the file to load without the extension.

        Returns
        -------
        data : dict
            The loaded data if it exists, otherwise None.
        """
        data = self._load_file(file_path, kw_args=kw_args)
        if data is not None:
            return data
        return None
