from abc import abstractmethod
from asyncio.streams import StreamReader
from datetime import datetime
import asyncio
import logging
import os
from typing import List, Callable
from pascal.parser import ParsedResult, Parser


class LauncherLike:
    @abstractmethod
    async def get(self) -> dict:
        pass

    async def get_parsed(self, parser: Parser) -> ParsedResult:
        return parser.parse(await self.get())


class ShellLauncher(LauncherLike):
    def __init__(
        self,
        cmd: str,
        stdout_cb: Callable = None,
        stderr_cb: Callable = None,
        timeout: float = None,
    ):
        self.cmd = cmd
        self.stdout_cb = stdout_cb
        self.stderr_cb = stderr_cb
        self.timeout = timeout

    async def get(self) -> dict:
        proc = await asyncio.create_subprocess_shell(
            "exec " + self.cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def read_stream(
            s: StreamReader, callback: Callable, outputs: list
        ) -> List[dict]:
            while True:
                data = await s.readline()
                if not data:
                    break
                output = {
                    "text": data.decode("utf-8").strip(),
                    "timestamp": str(datetime.now().timestamp()),
                }
                if isinstance(callback, Callable):
                    await callback(output)
                outputs.append(output)

        stdout, stderr = [], []
        tasks = [
            asyncio.create_task(read_stream(proc.stdout, self.stdout_cb, stdout)),
            asyncio.create_task(read_stream(proc.stderr, self.stderr_cb, stderr)),
        ]

        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.timeout)
        except asyncio.TimeoutError:
            logging.info("Process [%s] timeout. Trying to clean up." % self.cmd[:20])
            try:
                proc.terminate()
                await proc.communicate()
            except Exception as e:
                logging.warning("Failed to terminate the process. Reason: " + str(e))

        return {
            "stdout": stdout,
            "stderr": stderr,
            "cmd": self.cmd,
            "_module": self.__class__.__name__,
        }


if __name__ == "__main__":

    async def main():
        async def stdout_cb(x):
            print("stdout: " + x["text"])

        async def stderr_cb(x):
            print("stderr: " + x["text"])

        shell = ShellLauncher("sudo pcm-core.x -csv", stdout_cb, stderr_cb)
        await shell.get()

    asyncio.run(main())
