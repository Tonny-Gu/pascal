from abc import abstractmethod
from datetime import datetime
import argparse
import json
import base64
import logging
import subprocess
import asyncio
import re
import os
import csv
from typing import AsyncGenerator, List, Union
from ..launcher import ShellLauncher
from . import BufferedMonitor


class PcmMonitorBase(BufferedMonitor):
    def __init__(self, shell: ShellLauncher, batch=True) -> None:
        super().__init__()
        self.shell = shell
        self.counter = 0
        self.batch = batch
        self.header = []

    async def run(self) -> None:
        shell = self.shell
        outputs = []

        async def callback(data: dict) -> None:
            print(data)
            if not data["text"]:
                return
            
            digit_pct = float(sum(c.isdigit() for c in data["text"])) / len(data["text"])
            data["digit_pct"] = digit_pct

            if not self.batch:
                await self.buf_push_batch([data])
                return
            
            outputs.append(data)
            
            if digit_pct < 0.5 and ',' in data["text"] and outputs:
                await self.buf_push_batch(outputs)
                outputs.clear()

        shell.stdout_cb = callback

        try:
            await shell.get()
        except asyncio.CancelledError:
            logging.debug("%s terminated." % self.__class__.__name__)
            raise

    async def get(self) -> dict:
        data = await self.buf_pop_all()
        timelines = {}
        for row_data in data:
            row = next(csv.reader([row_data["text"]]))
            if len(row) <= 1:
                continue
            if row_data["digit_pct"] > 0.5:
                header = ["timestamp", "no."] + self.header
                if not timelines:
                    timelines = {k:[] for k in header}
                for k, v in zip(
                        header,
                        [row_data["timestamp"], self.counter] + row):
                    timelines[k].append(v)

                if not self.batch:
                    self.counter += 1
            else:
                if self.batch:
                    self.header = row
                    self.counter += 1
                else:
                    self.header = (
                        ["%s/%s" % (a, b) for a, b in zip(self.header, row)]
                        if self.header
                        else row
                    )
        return {"timelines": timelines, "_module": self.__class__.__name__}


class PcmCoreMonitor(PcmMonitorBase):
    def __init__(self) -> None:
        super().__init__(ShellLauncher("sudo pcm-core.x -csv 2>/dev/null"))


class PcmPcieMonitor(PcmMonitorBase):
    def __init__(self) -> None:
        super().__init__(ShellLauncher("sudo pcm-pcie.x -csv 2>/dev/null"))


class PcmNumaMonitor(PcmMonitorBase):
    def __init__(self) -> None:
        super().__init__(ShellLauncher("sudo pcm-numa.x -csv 2>/dev/null"))


class PcmMonitor(PcmMonitorBase):
    def __init__(self) -> None:
        super().__init__(ShellLauncher("sudo pcm-memory.x -csv 2>/dev/null"), False)


class PcmMonitor(PcmMonitorBase):
    def __init__(self) -> None:
        super().__init__(ShellLauncher("sudo pcm.x -csv 2>/dev/null"), False)
