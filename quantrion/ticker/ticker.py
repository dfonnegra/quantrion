from abc import ABC, ABCMeta


class TickerMeta(ABCMeta):
    _instances = None
    _classes = []

    def __new__(mcs, name, bases, attrs):
        cls = super().__new__(mcs, name, bases, attrs)
        mcs._classes.append(cls)
        return cls

    def __call__(cls, symbol: str, *args, **kwargs):
        if cls._instances is None:
            cls._instances = {}
        if symbol not in cls._instances:
            cls._instances[symbol] = super().__call__(symbol, *args, **kwargs)
        return cls._instances[symbol]


class Ticker(ABC, metaclass=TickerMeta):
    @property
    def symbol(self) -> str:
        return self._symbol

    def __init__(
        self,
        symbol: str,
    ) -> None:
        self._symbol = symbol
