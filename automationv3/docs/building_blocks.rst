Building Blocks
===============

BuildingBlocks are the primary script-facing interface for RVT execution.
Inside an ``.. rvt::`` directive, each form is evaluated and block functions are
resolved by name from the active block registry.

Quick Example
-------------

.. code-block:: rst

   .. rvt::

      (always-pass)
      (random-fail 0.1)
      (Wait 1)
      (SetupSimulation "mode" "nominal" "seed" "42")

How To Explore
--------------

1. Start with the base contract in ``automationv3.framework.block``.
2. Review core block modules for callable names, syntax rules, and examples.
3. Use the class-level docstrings as the authoritative usage reference.

Plugin-Contributed Docs
-----------------------

Plugins can ship their own reStructuredText docs under a plugin-local
``docs/`` directory. During Sphinx builds, these docs are auto-discovered from
installed ``automationv3.plugins.*`` packages and staged under
``docs/_generated/plugins``.

For pip-installed plugins, include the plugin ``docs/`` files as package data
so they are available in the installed environment.

Core Catalog
------------

.. list-table:: Built-in core blocks
   :header-rows: 1
   :widths: 22 34 44

   * - RVT Name
     - Class
     - Purpose
   * - ``Wait``
     - ``automationv3.plugins.core.wait.Wait``
     - Pause execution for N seconds.
   * - ``SetupSimulation``
     - ``automationv3.plugins.core.wait.SetupSimulation``
     - Emit setup result details and attach a sample setup log artifact.
   * - ``Table-Driven``
     - ``automationv3.plugins.core.wait.TableDriven``
     - Render tabular HTML output from headers/rows.
   * - ``always-pass``
     - ``automationv3.plugins.core.blocks.outcome.AlwaysPass``
     - Deterministic passing block for control and smoke testing.
   * - ``always-fail``
     - ``automationv3.plugins.core.blocks.outcome.AlwaysFail``
     - Deterministic failing block for negative-path validation.
   * - ``random-fail``
     - ``automationv3.plugins.core.blocks.outcome.RandomFail``
     - Probabilistic failure block for resilience and retry testing.

Reference Pages
---------------

.. toctree::
   :maxdepth: 2

   api/framework_block
   api/plugins_core_wait
   api/plugins_core_outcome
