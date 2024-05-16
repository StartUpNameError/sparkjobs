import json
import os
from collections import OrderedDict
from typing import Callable

_JSON_OPEN_METHODS = {
    ".json": open,
}


class JSONFileLoader:
    """Loader JSON files."""

    def exists(self, file_path: str) -> bool:
        """Checks if the file exists.

        Parameters
        ----------
        file_path: str
            The full path to the file to load without the '.json' extension.


        Returns
        -------
        True if file path exists, False otherwise.
        """
        for ext in _JSON_OPEN_METHODS:
            if os.path.isfile(file_path + ext):
                return True
        return False

    def _load_file(self, full_path: str, open_method: Callable) -> OrderedDict:
        if not os.path.isfile(full_path):
            return

        # By default the file will be opened with locale encoding on Python 3.
        # We specify "utf8" here to ensure the correct behavior.
        with open_method(full_path, "rb") as fp:
            payload = fp.read().decode("utf-8")

        return json.loads(payload, object_pairs_hook=OrderedDict)

    def load(self, file_path: str) -> OrderedDict | None:
        """Attempt to load the file path.

        Parameters
        ----------
        file_path: str
            The full path to the file to load without the '.json' extension.

        Returns
        -------
        json : dict
            The loaded data if it exists, otherwise None.
        """
        for ext, open_method in _JSON_OPEN_METHODS.items():
            data = self._load_file(file_path + ext, open_method)
            if data is not None:
                return data
        return None
