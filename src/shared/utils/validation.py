"""Functions to validate input and parameters."""


def check_keys(d: dict, keys: list[str]) -> None:
    for k in keys:
        if k not in d:
            raise ValueError("Key `{k}` not found in dict.")
