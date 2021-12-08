from abc import abstractmethod

from redis import Redis


class BaseType:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def read(self, client: Redis, key: str):
        pass


class Hash(BaseType):
    def __init__(self, field: str = "") -> None:
        super().__init__()
        self.field = field

    def read(self, client: Redis, key: str):
        if self.field:
            return client.hget(key, self.field)
        return client.hgetall(key)


class Set(BaseType):
    def read(self, client: Redis, key: str):
        # TODO sscan
        return client.smembers(key)


class Zset(BaseType):
    def __init__(self, min_: int, max_: int) -> None:
        super().__init__()
        self.min = min_
        self.max = max_

    def read(self, client: Redis, key: str):
        # TODO use limit and offset
        return client.zrangebyscore(
            key, self.min, self.max, withscores=True
        )
