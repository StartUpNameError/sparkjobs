def split_commas(ctx, param, value: str) -> list[str]:
    """Splits commas. Used as callback in click Options."""
    if not value or value is None or value == "None":
        return []

    return value.split(",")
