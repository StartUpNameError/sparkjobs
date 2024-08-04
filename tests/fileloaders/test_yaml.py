from shared.fileloaders import YAMLFileLoader
from shared.testing.fixtures import datadir


def test_load_yaml(datadir):
    filepath = datadir.join("example")
    loader = YAMLFileLoader()
    loader.load(file_path=filepath)
