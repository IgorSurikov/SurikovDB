from typing import Union, Callable, Any, Set


class Expression:
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
    }

    def __init__(self, value: Union[dict, int, str, list], context_args=None):
        if context_args is None:
            context_args = set()

        self.value = value
        self.context_args = context_args

    def parse(self) -> Callable[..., Any]:
        v = self.value

        if isinstance(v, str):
            if v.startswith("'") and v.endswith("'"):
                v = v.strip("'")
                return lambda **x: v

            elif v in self.math_function_map:
                return lambda: {
                    'fn': self.math_function_map[f'{v}']
                }

            elif isinstance(v, str) and not v.startswith("'") and not v.endswith("'"):
                self.context_args.add(v)
                return lambda **x: x[f'{v}']

            else:
                raise Exception(f'invalid string: {v}')

        if isinstance(v, int):
            return lambda **x: v

        if isinstance(v, list):
            try:
                func = Expression(v[0], self.context_args).parse()()['fn']
            except (TypeError, KeyError):
                raise Exception(f'invalid function: {v}')

            args = [Expression(i, self.context_args).parse() for i in v[1:]]
            return lambda **x: func(*[a.__call__(**x) for a in args])
