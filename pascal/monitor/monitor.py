from abc import abstractmethod
import asyncio
import logging
from typing import List

from pascal.parser import ParsedResult, Parser


class MonitorLike:
    @abstractmethod
    async def run(self) -> None:
        pass

    @abstractmethod
    async def reset(self) -> None:
        pass

    @abstractmethod
    async def get(self) -> dict:
        pass

    async def get_parsed(self, parser: Parser) -> ParsedResult:
        return parser.parse(await self.get())


class BufferedMonitor(MonitorLike):
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.buffer = []

    async def buf_push_batch(self, data: List[dict]) -> None:
        async with self.lock:
            logging.debug("%s sampled." % self.__class__.__name__)
            self.buffer.extend(data)

    async def buf_pop_all(self) -> List[dict]:
        async with self.lock:
            data = self.buffer
            self.buffer = []
            return data

    async def reset(self) -> None:
        await self.buf_pop_all()

    @abstractmethod
    async def run(self) -> None:
        pass

    @abstractmethod
    async def get(self) -> dict:
        pass
