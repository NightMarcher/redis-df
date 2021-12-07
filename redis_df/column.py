from redis import Redis
from typing import Optional, Union

from .dtype import Hash, List, Set, Zset


class Column:
    def __init__(
        self,
        name: str,
        dtype: Union[Hash, List, Set, Zset],
        tmpl: str,
        client: Optional[Redis] = None,
    ) -> None:
        self.name = name
        self.dtype = dtype
        self.tmpl = tmpl
        self.client = client

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.name}>"
