from __future__ import annotations
from abc import abstractmethod
from asyncio.streams import StreamReader
from datetime import datetime
import asyncio
from typing import List, Callable, Union
import re
import base64
import json


class ParsedResult:
    def __init__(self, data: dict) -> None:
        self.data = data

    def get(self) -> dict:
        return self.data

    def get_parsed(self, parser: Parser) -> ParsedResult:
        return parser.parse(self.data)


class Parser:
    @abstractmethod
    def parse(self, data: Union[dict, ParsedResult]) -> ParsedResult:
        pass


class PasFmtParser(Parser):
    pattern = re.compile("PasFmtDat:\[(?P<data>[A-Za-z0-9+\/=]+)\]")

    @staticmethod
    def parse(data: Union[dict, ParsedResult]) -> ParsedResult:
        if isinstance(data, ParsedResult):
            data = data.get()

        for s in ("stdout", "stderr"):
            for i in range(len(data[s])):
                output = data[s][i]
                segments = PasFmtParser.pattern.findall(output["text"])
                if len(segments) == 1:
                    output["data_pasfmt"] = json.loads(base64.b64decode(segments[0]))
        return ParsedResult(data)


if __name__ == "__main__":
    pas_data = base64.b64encode(json.dumps({"a": "b"}).encode("utf-8")).decode("utf-8")
    data = {"stdout": [{"text": "hello.PasFmtDat:[%s]" % pas_data}], "stderr": []}
    print(PasFmtParser.parse(data).get())
