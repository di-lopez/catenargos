import yaml


class Config:
    def __init__(self, target: str, config: str = "config.yml"):

        self.target = target
        with open(config) as f:
            self.config = yaml.safe_load(f)

    def __getattr__(self, name):
        return self.config[self.target][name]
