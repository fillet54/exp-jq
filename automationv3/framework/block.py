import importlib
import pkgutil

import automationv3.plugins

from . import edn


class BuildingBlock:
    """
    The 'BuildingBlock' of the automation framework. Registers as a function to
    be run during text execution.
    """

    def name(self):
        """Returns the name of the building block. The name is used
        as a first order lookup for the block"""
        return type(self).__name__

    def check_syntax(self, *args):
        """Returns True if this BuildingBlock can support the
        arguments and False otherwise"""
        return True

    def execute(self, *args):
        """Executes the block.

        Returns a BlockResult"""
        return BlockResult(False)

    def as_rst(self, *args):
        """Converts block with arguments to RST

        Note: We don't use the something like __repr_rst__
        here due to maintaining backwards compatibility. A
        block is sort of treated as a singleton which then
        provides member functions that take a specific list
        of arguments. A building block typically will
        get wrapped up in a class that

        """
        src = edn.writes(edn.List([self.name(), *args]))
        return (
            "\n".join(
                [
                    ".. code-block:: clojure",
                    "",
                    *["  " + line for line in src.splitlines()],
                ]
            )
            + "\n\n"
        )


class BuildingBlockInst:
    """Building block `instance` which packs block together with arguments

    This provides a mechanism to make a BuildingBlock
    more pythonic without breaking backwards compatibility.

    New blocks are free to implement either this or BuildingBlock.
    The framework will mostly be interfacing with blocks via this
    interface.
    """

    def __init__(self, block, args):
        self.block = block
        self.args = args

    def name(self):
        return self.block.name()

    def valid(self):
        return self.block.check_syntax(*self.args)

    def execute(self):
        return self.block.execute(*self.args)

    def __repr_rst__(self):
        return self.block.as_rst(*self.args)


class BlockResult(object):
    """
    The result of executing a BuildingBlock
    """

    def __init__(self, passed, stdout="", stderr=""):
        self.passed = passed
        self.stdout = stdout
        self.stderr = stderr

    def __bool__(self):
        return self.passed

    def __str__(self):
        result = "PASS" if self.passed else "FAIL"
        return f"<BlockResult: {result}, {self.stdout}, {self.stderr}>"


def find_block(form):
    name, *args = form

    for block in all_blocks:
        if block.name() == name and block.check_syntax(*args):
            return BuildingBlockInst(block, args)


def iter_namespace(ns_pkg):
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


discovered_plugins = {
    name: importlib.import_module(name)
    for finder, name, ispkg in iter_namespace(automationv3.plugins)
}

all_blocks = [block() for block in BuildingBlock.__subclasses__()]
