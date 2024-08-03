import yaml

from shared.fileloader import FileLoader


class YAMLFileLoader(FileLoader):
    """YAML file loader.

    Loads file with ".yaml" extension.
    """

    def __init__(self):
        super().__init__(loader=yaml.safe_load, ext=".yaml", open_method=open)
