"""Core plugin package.

Provides default building blocks shipped with automationv3.
"""

from .blocks import AlwaysFail, AlwaysPass, RandomFail
from .wait import SetupSimulation, TableDriven, Wait

__all__ = [
    "Wait",
    "SetupSimulation",
    "TableDriven",
    "AlwaysPass",
    "AlwaysFail",
    "RandomFail",
]
