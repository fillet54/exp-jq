Script Execution and Reporting
==============================

This page documents how an ``.rst`` test script is split, executed, and reported.

RST Chunking Model
------------------

Script chunking is handled by :func:`automationv3.framework.rst.parse_rst_chunks`.
It uses Docutils traversal (not regex splitting) to find ``.. rvt::`` nodes and
then returns ordered chunks with source line numbers:

1. ``text`` chunks: raw RST between RVT directives.
2. ``rvt`` chunks: the RVT directive body only.

The RVT directive implementation strips leading directive option lines from the
body so execution receives only Lisp forms.

API details: :doc:`api/framework_rst`.

.. mermaid::

   flowchart TD
       A["Input script.rst"] --> B["Docutils parse + RVT node visitor"]
       B --> C["Ordered chunks: text | rvt"]
       C --> D["text chunk: preserved as-is"]
       C --> E["rvt chunk: execute Lisp forms"]
       E --> F["Per-block rvt-result directives"]
       D --> G["result_document.rst"]
       F --> G

Execution Pipeline (Worker)
---------------------------

Worker job execution starts in :func:`automationv3.jobqueue.executor.run_job`,
which calls :func:`automationv3.framework.executor.run_script_document`.

API details: :doc:`api/framework_executor`, :doc:`api/executor`.

Within :func:`automationv3.framework.executor.run_script_document_text`:

1. ``on_script_begin`` observer callback is emitted.
2. Each chunk is processed in source order.
3. For ``text`` chunks:

   - Original RST is appended directly to ``result_document``.
   - Observer events ``on_text_chunk`` and ``on_content(..., mime_type=\"text/rst\")`` are emitted.

4. For ``rvt`` chunks:

   - ``on_rvt_start`` is emitted.
   - The body is parsed into forms and evaluated sequentially (implicit ``do`` behavior).
   - Blocks are injected into the Lisp environment via :func:`automationv3.framework.executor.build_script_env`.
   - Each block invocation emits ``on_block_start`` and ``on_block_end`` with timestamp and duration.
   - Each block produces at least one ``.. rvt-result::`` fragment (default via ``BlockResult.as_rst_directives`` or fallback formatting).
   - Generated result fragments are appended into ``result_document`` and emitted via ``on_content``.
   - ``on_rvt_result`` and ``on_rvt_end`` are emitted.

5. ``on_script_end`` is emitted with final pass/fail.

``rvt-result`` Shape
--------------------

Each block-level result is represented as RST:

1. ``.. rvt-result::`` directive with:

   - ``:status: pass|fail``
   - ``:timestamp: <UTC ISO timestamp>``
   - ``:duration: <seconds>``

2. Nested ``.. rvt::`` directive containing the invoked step form.
3. Nested ``.. code-block:: text`` containing output/error text.

This allows the final report document to preserve normal narrative RST and only
replace executable RVT sections with structured results.

Central/Worker Data Movement
----------------------------

.. mermaid::

   sequenceDiagram
       participant FE as Frontend/UI
       participant Q as JobQueue(SQLite)
       participant C as CentralServer
       participant W as WorkerServer

       FE->>Q: add_job(job_data)
       C->>Q: get_next_job()
       C->>W: POST /jobs (job payload)
       loop during execution
           W->>C: POST /workers/{id}/events {job_id,event}
           C->>C: append live_job_events
           C->>C: append event.rst_fragment to live document
       end
       W->>C: POST /workers/{id}/result {summary,artifacts,success}
       C->>Q: record_result(...)
       C->>Q: remove_job(job_id)
       loop reliable artifact sync
           C->>W: GET /artifacts/{job_id}/manifest
           W-->>C: {tree_sha, files[path,size,sha1]}
           C->>C: compare local files vs manifest
           C->>W: GET /artifacts/{job_id}/{path} for missing/mismatch
           C->>C: compute local artifact tree via fscache
           C->>C: verify local_tree_sha == worker_tree_sha
           C->>Q: mark_artifacts_downloaded(job_id) when match
       end

Synchronization Guarantees
--------------------------

The system now treats synchronization in two distinct reliability classes:

1. Live execution stream is **best effort**.

   - Worker observer events are sent continuously to central.
   - Transient network issues can drop or delay live updates.
   - This stream is used for in-progress UX and does not define final integrity.

2. Final result data is **guaranteed before artifacts are marked complete**.

   - Worker computes an artifact tree hash (``artifact_tree_sha``) over the per-job results folder.
   - Central fetches worker manifest metadata (path/size/sha1 + tree hash).
   - Central downloads only required files (missing or mismatched by size/hash).
   - Central computes its own artifact tree hash using ``fscache``.
   - If tree hashes do not match, central retries synchronization and does not mark the job's artifacts as downloaded.

This design gives low-latency live updates while preserving a strong integrity
check for finalized report data.

Reporting and Persistence
-------------------------

Worker-side artifacts per job:

1. ``summary.txt``: run summary.
2. ``result_document.rst``: full rendered RST result document.
3. ``result_document.html``: HTML rendering of the result document.
4. ``result.json``: summary payload + observer events + result document text.

Central-side reporting behavior:

1. Live observer events are stored in memory while a job is in progress.
2. Live RST fragments are accumulated into a progressive result document for UI display.
3. On final result submission, central merges live events/document into result payload when needed.
4. Artifact parity is verified against worker state before ``artifacts_downloaded`` is set.
5. Final payload is persisted to ``job_results`` and displayed in report/job output views.
