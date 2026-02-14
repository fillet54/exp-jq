import math
import operator as op
import copy
from itertools import islice, count, cycle
from collections.abc import Iterable

from .edn import read, Symbol, List, Vector


class Env(dict):
    def __init__(self, params=(), args=(), outer=None):
        self.outer = outer
        self.destructure(params, args)

    def destructure(self, params, args):
        if len(params) != len(args):
            raise TypeError(f"Invalid arguments[{args}] received. Expected [{params}]")
        self.update(zip(params, args))

    def __contains__(self, key):
        return super().__contains__(key) or key in self.outer

    def __getitem__(self, key):
        if super().__contains__(key):
            return super().__getitem__(key)
        elif self.outer is not None and key in self.outer:
            return self.outer[key]
        else:
            raise KeyError(f"{key} not found.")


def partition(n, seq):
    "Returns a lazy sequence of lists of n items each, at offsets step apart."
    return zip(*[islice(seq, start, None, n) for start in range(n)])


def assoc(m, *args):
    """assoc[iates]. When applied to a map returns a new map with key mapped
    to value. When applied to vector returns new vector with val set at index.
    Note that index must be < length of vector
    """
    m = copy.deepcopy(m)
    for k, v in partition(2, args):
        m[k] = v
    return m


def dissoc(m, *args):
    """dissoc[iate]. Returns a new map of the same (hashed/sorted) type,
    that does not contain a mapping for key(s)."""
    m = copy.deepcopy(m)
    for k in args:
        m.pop(k, None)
    return m


def standard_env():
    env = Env()

    # math functions
    env.update({k: v for k, v in vars(math).items() if not k.startswith("__")})

    env.update(
        {
            "+": op.add,
            "-": op.sub,
            "*": op.mul,
            "/": op.truediv,
            ">": op.gt,
            "<": op.lt,
            ">=": op.ge,
            "<=": op.le,
            "=": op.eq,
            "abs": abs,
            "append": op.add,
            "apply": lambda proc, args: proc(*args),
            "first": lambda x: next(islice(x, 0, None)),
            "rest": lambda x: islice(x, 1, None),
            "cons": lambda x, y: [x] + y,
            "eq?": op.is_,
            "expt": pow,
            "count": len,
            "list": lambda *x: List(x),
            "list?": lambda x: isinstance(x, list),
            "map": map,
            "max": max,
            "min": min,
            "not": op.not_,
            "nil?": lambda x: x is None,
            "some?": lambda x: x is not None,
            "number?": lambda x: isinstance(x, (int, float)),
            "print": print,
            "procedure?": callable,
            "round": round,
            "symbol?": lambda x: isinstance(x, Symbol),
            "cycle": cycle,
            "take": lambda n, coll: islice(coll, 0, n),
            "range": lambda: count(),
            "str": lambda *x: "".join([str(i) for i in x]),
            "partition": partition,
            "assoc": assoc,
            "dissoc": dissoc,
        }
    )
    return env


global_env = standard_env()


special_forms = {}


def get_special_form(symbol):
    if symbol in special_forms:
        return special_forms[symbol]

    for test, special_form_fn in special_forms.items():
        if callable(test) and test(symbol):
            return special_form_fn


def if_special_form(x, env):
    (_, test, then, _else) = x if len(x) == 4 else x + [None]
    exp = then if eval(test, env) else _else
    return eval(exp, env)


special_forms["if"] = if_special_form


def do_special_form(x, env):
    _, *expressions = x
    last = None
    for exp in expressions:
        last = eval(exp, env)
    return last


special_forms["do"] = do_special_form


def def_special_form(x, env):
    (_, symbol, exp) = x
    env[symbol] = eval(exp, env)


special_forms["def"] = def_special_form


def let_special_form(x, env):
    _, bindings, *exprs = x
    env = Env(outer=env)
    # binding to environment
    for binding, expr in zip(bindings[::2], bindings[1::2]):
        env[binding] = eval(expr, env)
    return eval(List(["do"] + exprs), env)


special_forms["let"] = let_special_form


def quote_special_form(x, env):
    _, form = x
    return form


special_forms["quote"] = quote_special_form


def create_function(name, sigs, env):
    sigs = {len(sig[0]):sig for sig in sigs}
    def fn(*args):
        arity = len(args)
        if arity not in sigs:
            raise RuntimeError(f"Cannot call {name} with {arity} arguments")
        params, *exprs = sigs[arity]
        return eval(List([Symbol("do"), *exprs]), Env(params, args, outer=env))
    
    if name is not None:
        fn.__name__ = name
    return fn

def fn_special_form(x, env):
    _, *args = x
    name = args[0] if isinstance(args[0], Symbol) else None
    sigs = args[1:] if name else args
    if isinstance(sigs[0], Vector):
        sigs = List([sigs])
    elif not isinstance(sigs[0], List):
        raise ValueError(f"Parameter declaration {sigs[0]} should be a Vector")

    # validate all forms
    for sig in sigs:
        if not isinstance(sig[0], Vector):
            raise ValueError(f"Parameter declaration {sig[0]} should be a Vector")

    return create_function(name, sigs, env)


special_forms["fn"] = fn_special_form


def defn_special_form(x, env):
    _, name, *_ = x
    fn = fn_special_form(x, env)
    return eval(List([Symbol("def"), name, fn]))


special_forms["defn"] = defn_special_form


def has_leading_dot(symbol):
    result = symbol.startswith(".")
    return result


def dot_special_form(x, env):
    attr, sym, *args = x
    attr = attr[1:]  # remove leading dot

    is_property = attr.startswith("-")
    if is_property:
        attr = attr[1:]

    if isinstance(sym, Symbol):
        # Get symbol from environment
        the_attr = getattr(env[sym], attr)
    else:
        val = eval(sym)
        the_attr = getattr(val, attr)

    if is_property:
        return the_attr
    return the_attr(*args)


special_forms[has_leading_dot] = dot_special_form


def eval(x, env=global_env):
    "Evaluate an expression in an environment."

    # symbol reference
    if isinstance(x, Symbol):
        return env[x]

    # constant
    elif not isinstance(x, List):
        return x

    # special forms
    special_form = get_special_form(x[0])
    if special_form:
        return special_form(x, env)

    # procedure call
    else:
        proc = eval(x[0], env)
        args = [eval(arg, env) for arg in x[1:]]
        return proc(*args)


def eval_text(s):
    result = eval(read(s))
    if not isinstance(result, str) and isinstance(result, Iterable):
        return List(result)
    return result


if __name__ == "__main__":

    results = eval_text(
        """
(do
    (def fizzbuzz (fn [n]
      (let [fizzes (cycle ["" "" "Fizz"])
            buzzes (cycle ["" "" "" "" "Buzz"])
            words (map str fizzes buzzes)
            numbers (map str (rest (range)))]
        (take n (map max words numbers)))))

    (fizzbuzz 50))
"""
    )

    for r in results:
        print(r)


