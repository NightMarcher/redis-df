from abc import abstractmethod


class BaseType:
    def __init__(self, client, key: str) -> None:
        self.client = client
        self.key = key

    @abstractmethod
    def read(self):
        pass


class Hash(BaseType):
    def read(self, field=""):
        if field:
            return self.client.hget(self.key, field)
        return self.client.hgetall(self.key)


class List(BaseType):
    def read(self):
        return self.client.lrange(self.key, 0, -1)


class Set(BaseType):
    def read(self):
        # TODO sscan
        return self.client.smembers(self.key)
