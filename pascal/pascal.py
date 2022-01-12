import asyncio
import os
import logging
from datetime import datetime
import string
import json
from typing import Callable, Iterable, List
from functools import wraps
from pathlib import Path
from pascal.schema import Schema
import itertools

_ctx = {}
_exps = []


def _param_gen(fields: dict):
    names = fields.keys()
    vals = fields.values()
    for param_set in itertools.product(*vals):
        yield dict(zip(names, param_set))


def _init():
    exp_name = "exp_%s_%s" % (
        _ctx["exp_name"],
        datetime.now().strftime("%Y-%m-%d-%H-%M"),
    )
    _ctx["exp_full_name"] = exp_name

    if not os.path.exists(exp_name):
        os.mkdir(exp_name)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("%s/output.log" % exp_name),
            logging.StreamHandler(),
        ],
    )

    logging.info("Pascal started.")


def _finalize():
    exp_item = "%s_%s" % (_ctx["exp_item"], datetime.now().strftime("%Y-%m-%d-%H-%M"))

    with open("%s/%s.json" % (_ctx["exp_full_name"], exp_item), "w") as f:
        f.write(json.dumps(_ctx["result"]))
    
    logging.info("%s finished." % _ctx["exp_item"])


def apply_schema(schema: Schema):
    def decorate(func: Callable):
        assert func.__globals__["__name__"] == "__main__"
        if "exp_name" not in _ctx:
            exp_name = Path(func.__globals__["__file__"]).stem
            _ctx["exp_name"] = exp_name
            _init()

        @wraps(func)
        def wrapper():
            async def main():
                func_argnames = func.__code__.co_varnames[: func.__code__.co_argcount]
                func_params = {i: func._params[i] for i in func_argnames}
                cmd_params = {
                    k: v for k, v in func._params.items() if k not in func_params
                }

                task = asyncio.create_task(schema.run())
                for param_set in _param_gen(func_params):
                    logging.info(
                        "%s started to run with params %s"
                        % (func.__name__, param_set)
                    )
                    _ctx.update(
                        {
                            "schema": schema,
                            "func": func,
                            "cmd_params": cmd_params,
                            "func_params": param_set,
                            "func_name": func.__name__,
                        }
                    )
                    await func(**param_set)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logging.debug("Pascal recevied stopped signal.")
                ret = {"exp_item": func.__name__, "result": schema.get()}
                _ctx.update(ret)
                _finalize()
                return ret

            return asyncio.run(main())

        _exps.append(wrapper)
        return wrapper

    return decorate


def parametrize(name: str, values: Iterable):
    def decorate(func: Callable):
        if not hasattr(func, "_params"):
            func._params = {}
        func._params[name] = values
        return func

    return decorate


_formatter = string.Formatter()


async def shell(
    cmds: List[str], *other_cmds: List[str], timeout: int = None, params={}
):
    assert _ctx
    schema: Schema = _ctx["schema"]
    all_params = dict(_ctx["cmd_params"])
    all_params.update(params)
    cmd_groups = (cmds,) + other_cmds
    ret_values = []
    for cmd_group in cmd_groups:
        fields = {}
        for cmd in cmd_group:
            field_names = [i for _, i, _, _ in _formatter.parse(cmd)]
            if field_names[0]:
                fields.update({i: all_params[i] for i in field_names})

        for param_set in _param_gen(fields):
            param_set.update(_ctx["func_params"])
            new_cmds = [cmd.format(**param_set) for cmd in cmd_group]
            logging.info("%s launched." % new_cmds)
            ret = await schema.shell(new_cmds, timeout)
            ret_values.append(ret)
    return ret_values


def run_all():
    for exp in _exps:
        exp()
