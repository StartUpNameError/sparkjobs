from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shared.fileloaders import YAMLFileLoader
from shared.instantiator import Instatiator


@dataclass
class Deserializable:
    name: str
    clspath: str
    kwargs: dict[str, str] | None = None


class ConfigParser:
    def __init__(self, data, context: dict[str, Any] | None = None) -> None:
        self.data = data
        self.context = context or {}

        self._instantiator = Instatiator(context)

    @classmethod
    def from_yaml(
        cls,
        yamfile,
        context: dict[str, Any] | None = None,
    ) -> ConfigParser:

        loader = YAMLFileLoader()
        data = loader.load(file_path=yamfile)
        return cls(data=data, context=context)

    def parse_one(self, o: Deserializable) -> Any:
        return self._instantiator.instantiate(clspath=o.clspath)

    def parse(self, key: str) -> dict[str, Any]:

        parsed_objects: dict[str, Any] = {}

        for data in self.data[key]:
            o = Deserializable(**data)
            parsed_objects[o.name] = self.parse_one(o=o)

        return parsed_objects
