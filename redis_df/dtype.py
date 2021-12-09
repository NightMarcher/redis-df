from abc import abstractmethod

from pandas import DataFrame, Series
from redis import Redis
from typing import Callable, Optional, Union


class BaseType:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def read(self, client: Redis, key: str, name: str):
        pass


class Hash(BaseType):
    def __init__(self, value_parser: Optional[Callable] = None) -> None:
        super().__init__()
        self.value_parser = value_parser

    def read(self, client: Redis, key: str, name: str) -> Union[DataFrame, Series]:
        raw_dict = client.hgetall(key)
        if self.value_parser:
            parsed_dict = {pk: self.value_parser(
                value) for pk, value in raw_dict.items()}
            return DataFrame.from_dict(parsed_dict, orient='index')
        return Series(raw_dict, name=name)


class Set(BaseType):
    def __init__(self, boolean: bool = True) -> None:
        super().__init__()
        self.boolean = boolean

    def read(self, client: Redis, key: str, name: str) -> Series:
        # TODO sscan
        raw_set = client.smembers(key)
        return Series(data=[self.boolean] * len(raw_set), index=raw_set, name=name)


class Zset(BaseType):
    def __init__(self, min_: int, max_: int) -> None:
        super().__init__()
        self.min = min_
        self.max = max_

    def read(self, client: Redis, key: str, name: str) -> Series:
        # TODO use limit and offset
        raw_tuples = client.zrangebyscore(
            key, self.min, self.max, withscores=True
        )
        return Series({k: v for (k, v) in raw_tuples}, name=name)
