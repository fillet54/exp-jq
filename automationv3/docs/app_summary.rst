AutomationV3 App Summary and Requirements
=========================================

What This App Does
------------------

AutomationV3 is a script-driven test execution platform for hardware/software
validation workflows. It lets users:

- author tests as reStructuredText (``.rst``) with executable ``.. rvt::`` steps,
- queue tests to workers,
- stream live execution progress,
- collect artifacts/results reliably,
- organize formal verification runs into reports tied to requirements,
- run ad-hoc development tests in a scratch report.

The system is optimized for two operating modes:

- formal report execution for requirement traceability,
- rapid local iteration during script development.

Core Domain Concepts
--------------------

- ``Framework``: parses script documents, executes RVT forms, injects blocks, emits execution output/events.
- ``JobQueue``: queues jobs, dispatches to workers, records outcomes, tracks in-progress and completed state.
- ``Reporting``: links jobs/scripts/requirements/UUT context into report-level views and exports.
- ``UUT Store / FS Cache``: stores UUT snapshots and validates artifact tree parity across worker and central.
- ``Frontend / TUI``: web and terminal interfaces for queueing, monitoring, and reviewing results.

End-to-End Flow
---------------

1. User submits one or more scripts.
2. Script metadata and optional variations are expanded to 1..N jobs.
3. Jobs are queued and dispatched to workers.
4. Worker executes script:

   - preserves narrative RST chunks,
   - executes RVT forms block-by-block,
   - emits observer events and block ``rvt-result`` output.

5. Live events stream to central (best effort).
6. Final result + artifacts are synchronized and verified (guaranteed by tree-hash parity).
7. Results appear in report views, scratch view, and per-job output pages.

Detailed Functional Requirements
--------------------------------

1) Script/Framework Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Parse ``.rst`` via docutils and identify executable ``.. rvt::`` regions without regex-only splitting.
- Preserve non-RVT text exactly in result documents.
- Execute RVT forms in sequence (implicit ``do`` behavior).
- Inject registered building blocks into the Lisp environment.
- Support variation directives that expand a script into multiple jobs.
- Track syntax/reader errors with source line/column and report-friendly output.
- Emit one ``rvt-result`` per block invocation (not one per RVT directive).
- Render each block source in result docs via block ``as_rst()`` output.
- Default ``as_rst()`` formatting:

  - keep single-line output when it fits within 80 chars,
  - split and indent only when over limit.

- Allow blocks to emit attachments and custom result fragments through result interfaces.

2) Job Queue and Worker Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Persist queued jobs and completed results in SQLite.
- Support queueing from:

  - script list,
  - requirement groups,
  - suites,
  - report actions (requeue all/script/requirement).

- Preserve report and UUT context in job payload.
- Stream live worker observer events to central for in-progress visibility.
- Record final result payload including result document and observer event history.
- Support pagination and scalable listing for large completed history.

3) Reliability and Data Sync Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Treat live event streaming as best effort.
- Treat final artifacts/results as guaranteed before completion is marked.
- Use worker manifest + file hashes + fscache tree hashes to confirm exact parity.
- Download only missing/mismatched files during sync.
- Mark ``artifacts_downloaded`` only after tree parity succeeds.
- Ensure no critical result data exists only in transient live stream state.

4) Reporting Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~

- Support report lifecycle:

  - create,
  - delete (including associated jobs/results),
  - clear results while keeping script references.

- Track requirements explicitly per report.
- Auto-add requirements when scripts are queued to a report.
- Allow removing requirements and removing scripts from reports.
- Show requirement-centered status for formal reports:

  - pass / partial / fail / not run semantics,
  - per-script latest run and variation indicators.

- Provide per-job output pages with rendered result document + raw output access.
- Provide export to PDF with summary sections and report metadata.

5) Scratch Report Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Scratch report is a special ad-hoc report (``__scratch__``) for development runs.
- Scratch detail page should **not** be grouped by requirements.
- Scratch detail page should show a flat list ordered by **finish time (newest first)**.
- Rows should show:

  - completed time,
  - status,
  - script title/path,
  - UUT,
  - worker,
  - output link.

6) Frontend Requirements
~~~~~~~~~~~~~~~~~~~~~~~~

- Web pages for:

  - queue overview,
  - workers,
  - reports list/detail/export,
  - scripts list/detail,
  - job output.

- Script views:

  - rendered view (default),
  - raw source view,
  - requirement-aware browsing and directory browsing.

- Report detail actions:

  - requeue all/script/requirement,
  - clear results,
  - delete report.

- Display syntax issues and execution failures clearly with status badges.

7) Local TUI Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~

- Cross-platform terminal interface (Windows-friendly).
- Persist local configuration (scripts root, UUT) across runs using TOML.
- Fast script selection with prompt-driven flow.
- Scratch-first execution mode for local single-worker use.
- Live current-run view with:

  - RST->ANSI rendering,
  - active step timing,
  - per-step pass/fail with timestamps,
  - replay of current/last run output.

- Preserve multiline RVT source formatting in live output.
- Render admonitions and key RST structures in ANSI output.

Non-Functional Requirements
---------------------------

- **Reliability**: deterministic final artifact/result integrity checks.
- **Traceability**: explicit linkage between reports, scripts, requirements, UUT context, and runs.
- **Usability**: lightweight scratch workflow for script development.
- **Performance**: handle hundreds to ~1000 scripts without complex indexing infrastructure.
- **Extensibility**: plugin-oriented building blocks and generated block documentation.
- **Offline Support**: documentation assets available without internet dependencies.

Scope Boundaries
----------------

- The system is currently optimized for practical execution/reporting workflows, not full enterprise RBAC/multi-tenant policy controls.
- Database migration continuity is not always required in current development mode; clean-start workflows are supported.
- Artifact rendering extensibility exists, but advanced artifact viewers may evolve over time.

Canonical References
--------------------

- ``automationv3/docs/architecture.rst``
- ``automationv3/docs/execution_reporting.rst``
- ``automationv3/docs/sql_schemas.rst``
- ``automationv3/docs/building_blocks.rst``
