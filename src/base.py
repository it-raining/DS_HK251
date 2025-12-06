from abc import ABC, abstractmethod

class BasePlugin(ABC):
    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def finalize(self):
        pass