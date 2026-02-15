import random

from automationv3.framework.block import BlockResult, BuildingBlock


class AlwaysPass(BuildingBlock):
    def name(self):
        return "always-pass"

    def check_syntax(self, *args):
        return len(args) == 0

    def execute(self):
        return BlockResult(True, stdout="always-pass")


class AlwaysFail(BuildingBlock):
    def name(self):
        return "always-fail"

    def check_syntax(self, *args):
        return len(args) == 0

    def execute(self):
        return BlockResult(False, stderr="always-fail")


class RandomFail(BuildingBlock):
    def name(self):
        return "random-fail"

    def check_syntax(self, *args):
        if len(args) != 1:
            return False
        try:
            float(args[0])
            return True
        except (TypeError, ValueError):
            return False

    def execute(self, probability):
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

