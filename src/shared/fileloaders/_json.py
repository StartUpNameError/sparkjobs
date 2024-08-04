import json

from shared.fileloader import FileLoader


class JSONFileLoader(FileLoader):
    """JSON file loader.

    Loads file with ".json" extension.
    """

    def __init__(self):
        super().__init__(loader=json.loads, ext=".json", open_method=open)
