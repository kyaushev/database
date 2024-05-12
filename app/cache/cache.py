import json

from app.utils.singleton import MetaSingleton


class ConfigCache(metaclass=MetaSingleton):
    def __init__(self) -> None:
        file = open("app\config\config_replicas.json")
        configurations = json.load(file)
        self.replicas = configurations["replicas"]
