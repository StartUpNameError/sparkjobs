from shared.fileloaders import JSONFileLoader
from shared.testing.fixtures import datadir


def test_load_json(datadir):
    filepath = datadir.join("example")
    loader = JSONFileLoader()
    loader.load(file_path=filepath)
