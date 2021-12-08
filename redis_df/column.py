from redis import Redis
from typing import Callable, Optional, Union

from .dtype import Hash, Set, Zset


class Column:
    def __init__(
        self,
        name: str,
        dtype: Union[Hash, Set, Zset],
        tmpl: str,
        client: Optional[Redis] = None,
        converter: Optional[Callable] = None,
        default: Union[str, int] = None,
    ) -> None:
        self.name = name
        self.dtype = dtype
        self.tmpl = tmpl
        self.client = client
        self.converter = converter
        self.default = default

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.name}>"
