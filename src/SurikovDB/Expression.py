from typing import Union, Callable, Any, Set, Tuple
from SurikovDB.constants import *

class Expression:
    json_schema = {
        "anyOf": [
            {"type": "string"},
            {"type": "number"},
            {"type": "boolean"},
            {"type": "array", "minItems": 2},
        ]
    }

    math_function_map = {
        'add': lambda *x: x[0] + x[1],
        'sub': lambda *x: x[0] - x[1],
        'mul': lambda *x: x[0] * x[1],
        'div': lambda *x: x[0] / x[1],
        'eq': lambda *x: x[0] == x[1],
        'gt': lambda *x: x[0] > x[1],
        'ge': lambda *x: x[0] >= x[1],
        'lt': lambda *x: x[0] < x[1],
        'le': lambda *x: x[0] <= x[1],
        'ne': lambda *x: x[0] != x[1],
        'and': lambda *x: x[0] and x[1],
        'or': lambda *x: x[0] or x[1],
        'to_str': lambda *x: str(x[0]),
        'to_int': lambda *x: int(x[0]),
    }

    def __init__(self, value: Union[dict, int, str, list], context_args=None):
        if context_args is None:
            context_args = set()

        self.value = value
        self.context_args = context_args

    def parse(self) -> Tuple[Callable[..., Any], Set[str]]:
        v = self.value

        if isinstance(v, str):
            if v.startswith("'") and v.endswith("'"):
                v = v.strip("'")
                return lambda **x: v, self.context_args

            elif v in self.math_function_map:
                return lambda: {
                    'fn': self.math_function_map[f'{v}']
                }, self.context_args

            elif isinstance(v, bool):
                return lambda **x: v, self.context_args

            elif isinstance(v, str) and not v.startswith("'") and not v.endswith("'"):
                self.context_args.add(v)

                def f(**x):
                    try:
                        return x[f'{v}']
                    except KeyError as e:
                        raise Exception(f'No column: {e.args[0]}')

                return lambda **x: f(**x), self.context_args

            else:
                raise Exception(f'invalid string: {v}')

        if isinstance(v, int):
            return lambda **x: v, self.context_args

        if isinstance(v, list):
            try:
                func = Expression(v[0], self.context_args).parse()[0]()['fn']
            except (TypeError, KeyError):
                raise Exception(f'invalid function: {v}')

            args = [Expression(i, self.context_args).parse()[0] for i in v[1:]]
            return lambda **x: func(*[a(**x) for a in args]), self.context_args
