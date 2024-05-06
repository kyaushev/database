import json

from app.utils.singleton import MetaSingleton

class ConfigCache(metaclass=MetaSingleton):
    def __init__(self) -> None:
        file = open("app\config\config.json")
        configurations = json.load(file)
        self.shards = configurations["shards"]
            