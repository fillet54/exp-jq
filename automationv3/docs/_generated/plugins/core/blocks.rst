Core Plugin BuildingBlocks
==========================

This page documents BuildingBlocks contributed by the core plugin package.

Usage Notes
-----------

- RVT function names are defined by each block's ``name()`` method.
- Syntax checks are implemented by each block's ``check_syntax()`` method.
- Execution output is represented through ``BlockResult`` and rendered as
  ``rvt-result`` directives in reports.

wait.py
-------

.. automodule:: automationv3.plugins.core.wait
   :members:
   :undoc-members:
   :show-inheritance:

blocks/outcome.py
-----------------

.. automodule:: automationv3.plugins.core.blocks.outcome
   :members:
   :undoc-members:
   :show-inheritance:
