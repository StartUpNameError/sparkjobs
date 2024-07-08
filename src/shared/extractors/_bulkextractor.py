from shared.extractor import DataFrame, Extractor


class BulkExtractor:

    def __init__(self):
        self._extractors = {}

    def update(self, extractors: dict[str, Extractor]) -> None:
        self._extractors.update(extractors)

    def add(self, key: str, extractor: Extractor) -> None:
        self._extractors[key] = extractor

    def extract_one(self, key: str):
        if key not in self._extractors:
            raise

        return self._extractors[key].extract()

    def extract_all(self) -> dict[str, DataFrame]:

        all_ = {}
        for key in self._extractors:
            all_[key] = self.extract_one(key)

        return all_
