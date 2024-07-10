from shared.extractor import Extractor
from shared.extractors import BulkExtractor
from shared.typing import DataFrame


def extract(extractors: dict[str, Extractor]) -> dict[str, DataFrame]:
    """Calls specified extractors.

    Parameters
    ----------
    extractors : dict, str -> Extractor
        Extractors to call.

    Returns
    -------
    dict : dict, str -> DataFrame
    """
    extractor = BulkExtractor()
    extractor.update(extractors)
    return extractor.extract_all()
