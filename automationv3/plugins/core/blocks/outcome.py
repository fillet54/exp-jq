"""Outcome-oriented BuildingBlocks used for control-flow and failure simulation.

These blocks are intentionally simple and deterministic (except ``random-fail``)
so users can validate execution/reporting behavior without depending on external
systems.
"""

import random

from automationv3.framework.block import BlockResult, BuildingBlock


class AlwaysPass(BuildingBlock):
    """Return a passing result with no arguments.

    Usage in ``.. rvt::``:

    ``(always-pass)``
    """

    def name(self):
        """Expose the Lisp function name for this block."""
        return "always-pass"

    def check_syntax(self, *args):
        """Accept only zero-arity calls."""
        return len(args) == 0

    def execute(self):
        """Always return a successful :class:`~automationv3.framework.block.BlockResult`."""
        return BlockResult(True, stdout="always-pass")


class AlwaysFail(BuildingBlock):
    """Return a failing result with no arguments.

    Usage in ``.. rvt::``:

    ``(always-fail)``
    """

    def name(self):
        """Expose the Lisp function name for this block."""
        return "always-fail"

    def check_syntax(self, *args):
        """Accept only zero-arity calls."""
        return len(args) == 0

    def execute(self):
        """Always return a failed :class:`~automationv3.framework.block.BlockResult`."""
        return BlockResult(False, stderr="always-fail")


class RandomFail(BuildingBlock):
    """Fail stochastically based on a provided probability.

    The argument can be either:

    - ``0.0`` to ``1.0`` (fraction form), or
    - ``1`` to ``100`` (percent form).

    Usage in ``.. rvt::``:

    - ``(random-fail 0.10)`` -> 10%% fail probability
    - ``(random-fail 25)`` -> 25%% fail probability
    """

    def name(self):
        """Expose the Lisp function name for this block."""
        return "random-fail"

    def check_syntax(self, *args):
        """Require exactly one numeric argument."""
        if len(args) != 1:
            return False
        try:
            float(args[0])
            return True
        except (TypeError, ValueError):
            return False

    def execute(self, probability):
        """Execute one random trial and return pass/fail in a ``BlockResult``.

        Parameters
        ----------
        probability:
            Failure probability in fraction or percent form.
        """
        p = float(probability)
        # Accept either [0,1] (fraction) or (1,100] (percent).
        if p > 1.0:
            p = p / 100.0
        p = max(0.0, min(1.0, p))
        roll = random.random()
        failed = roll < p
        detail = f"random-fail p={p:.4f} roll={roll:.4f}"
        if failed:
            return BlockResult(False, stderr=detail)
        return BlockResult(True, stdout=detail)
