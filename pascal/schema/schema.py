from abc import abstractmethod
from asyncio.streams import StreamReader
from datetime import datetime
import asyncio
import logging
from typing import List, Callable, Union
from pascal.launcher import ShellLauncher
from pascal.monitor import (
    MonitorLike,
    PcmCoreMonitor,
    PcmMonitor,
    PcmNumaMonitor,
    PcmPcieMonitor,
)
from pascal.parser import PasFmtParser


class Schema:
    def __init__(self) -> None:
        pass

    @abstractmethod
    async def run(self) -> dict:
        pass

    @abstractmethod
    async def shell(self, cmds: List[str], timeout: int = None) -> dict:
        pass

    @abstractmethod
    def get(self) -> List[dict]:
        pass


class MultiProcessSchema(Schema):
    def __init__(self) -> None:
        super().__init__()
        self.results = []
        self.job_id = 0
        self.monitors: List[MonitorLike] = [
            # PcmMonitor(),
            PcmPcieMonitor(),
            #  PcmNumaMonitor(),
            # PcmCoreMonitor(),
        ]

    async def run(self) -> dict:
        tasks = [asyncio.create_task(i.run()) for i in self.monitors]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logging.debug("%s recevied stopped signal." % self.__class__.__name__)
            [i.cancel() for i in tasks]
            raise

    async def shell(self, cmds: List[str], timeout: int = None) -> dict:
        def callback(pipe_name: str, proc_id: int):
            job_id = self.job_id

            async def fun(output: dict):
                msg: str = output["text"]
                logging.info(
                    "(Job %d, Proc %d, %s) %s: '%s'."
                    % (job_id, proc_id, pipe_name, cmds[proc_id][:20], msg.strip()[:100])
                )

            return fun

        [await i.reset() for i in self.monitors]

        handlers = [
            ShellLauncher(
                cmds[i], callback("stdout", i), callback("stderr", i), timeout
            ).get_parsed(PasFmtParser)
            for i in range(len(cmds))
        ]

        result = {
            "cmds": cmds,
            "outputs": [i.get() for i in await asyncio.gather(*handlers)],
            "monitors": [await i.get() for i in self.monitors],
        }

        self.job_id += 1
        self.results.append(result)
        return result

    def get(self) -> List[dict]:
        return self.results
