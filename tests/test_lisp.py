import pytest

from automationv3.framework import edn, lisp


def eval_expr(src, env=None):
    local_env = env if env is not None else lisp.Env(outer=lisp.global_env)
    return lisp.eval(edn.read(src), local_env)


def test_env_destructure_mismatched_arity_raises():
    with pytest.raises(TypeError, match="Invalid arguments"):
        lisp.Env(params=[edn.Symbol("x")], args=[])


def test_env_lookup_uses_outer_scope():
    outer = lisp.Env()
    outer[edn.Symbol("x")] = 42
    env = lisp.Env(outer=outer)

    assert edn.Symbol("x") in env
    assert env[edn.Symbol("x")] == 42


def test_partition_groups_values_by_size():
    pairs = list(lisp.partition(2, [1, 2, 3, 4]))
    assert pairs == [(1, 2), (3, 4)]


def test_assoc_returns_new_mapping_without_mutating_input():
    original = {"a": 1}
    updated = lisp.assoc(original, "b", 2)

    assert original == {"a": 1}
    assert updated == {"a": 1, "b": 2}


def test_dissoc_returns_new_mapping_without_mutating_input():
    original = {"a": 1, "b": 2}
    updated = lisp.dissoc(original, "a")

    assert original == {"a": 1, "b": 2}
    assert updated == {"b": 2}


def test_get_special_form_resolves_direct_and_predicate_forms():
    assert lisp.get_special_form("if") is lisp.if_special_form
    assert lisp.get_special_form(".upper") is lisp.dot_special_form


def test_eval_symbol_lookup_and_missing_symbol_error():
    env = lisp.Env(outer={})
    env[edn.Symbol("x")] = 7
    assert lisp.eval(edn.Symbol("x"), env) == 7

    with pytest.raises(KeyError, match="missing not found"):
        lisp.eval(edn.Symbol("missing"), env)


def test_eval_if_form_with_and_without_else():
    assert eval_expr("(if true 1 2)") == 1
    assert eval_expr("(if false 1 2)") == 2
    assert eval_expr("(if false 1)") is None


def test_eval_do_returns_last_expression():
    assert eval_expr("(do 1 2 3)") == 3


def test_eval_def_sets_binding_in_environment():
    env = lisp.Env(outer=lisp.global_env)
    result = eval_expr("(def x 12)", env)

    assert result is None
    assert env[edn.Symbol("x")] == 12


def test_eval_let_binds_values_and_does_not_leak():
    env = lisp.Env(outer=lisp.global_env)
    result = eval_expr("(let [x 2 y 3] (+ x y))", env)

    assert result == 5
    assert not dict.__contains__(env, edn.Symbol("x"))
    assert not dict.__contains__(env, edn.Symbol("y"))


def test_eval_quote_returns_raw_form():
    quoted = eval_expr("(quote (a b c))")
    assert isinstance(quoted, edn.List)
    assert quoted == [edn.Symbol("a"), edn.Symbol("b"), edn.Symbol("c")]


def test_eval_fn_and_call_via_def():
    result = eval_expr("(do (def inc1 (fn [x] (+ x 1))) (inc1 10))")
    assert result == 11


def test_eval_fn_zero_arity():
    result = eval_expr("(do (def zero (fn [] 42)) (zero))")
    assert result == 42


def test_eval_defn_single_arity():
    result = eval_expr("(do (defn twice [x] (* x 2)) (twice 6))")
    assert result == 12


def test_eval_defn_zero_arity():
    result = eval_expr("(do (defn zero [] 42) (zero))")
    assert result == 42


def test_eval_defn_multi_arity_dispatch():
    result1 = eval_expr("(do (defn f ([x] (+ x 1)) ([x y] (+ x y))) (f 4))")
    result2 = eval_expr("(do (defn f ([x] (+ x 1)) ([x y] (+ x y))) (f 4 5))")

    assert result1 == 5
    assert result2 == 9


def test_eval_defn_multi_arity_including_zero_arity():
    result0 = eval_expr("(do (defn f ([] 10) ([x] (+ x 1))) (f))")
    result1 = eval_expr("(do (defn f ([] 10) ([x] (+ x 1))) (f 4))")

    assert result0 == 10
    assert result1 == 5


def test_eval_defn_arity_error():
    with pytest.raises(RuntimeError, match="Cannot call f with 0 arguments"):
        eval_expr("(do (defn f ([x] x) ([x y] y)) (f))")


def test_eval_fn_invalid_parameter_declaration_raises():
    with pytest.raises(ValueError, match="should be a Vector"):
        eval_expr("(fn x (+ x 1))")


def test_eval_closure_with_captured_value():
    result = eval_expr(
        "(do (def make-adder (fn [x] (fn plusx [y] (+ x y)))) "
        "(def add3 (make-adder 3)) (add3 4))"
    )
    assert result == 7


def test_eval_dot_method_call_and_property_access():
    method_result = eval_expr('(do (def s "abc") (.upper s))')
    property_result = eval_expr("(.-real pi)")

    assert method_result == "ABC"
    assert property_result == pytest.approx(3.141592653589793)


def test_eval_dot_unknown_attribute_raises():
    with pytest.raises(AttributeError):
        eval_expr('(do (def s "abc") (.does_not_exist s))')


def test_eval_procedure_call_non_callable_raises():
    with pytest.raises(TypeError):
        eval_expr("(do (def x 1) (x 2))")


def test_eval_text_wraps_iterables_as_list():
    result = lisp.eval_text("(take 3 (range))")
    assert isinstance(result, edn.List)
    assert result == [0, 1, 2]


def test_eval_text_keeps_strings_as_strings():
    result = lisp.eval_text('(str "a" "b" "c")')
    assert isinstance(result, str)
    assert result == "abc"
