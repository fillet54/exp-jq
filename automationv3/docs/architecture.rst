Architecture
============

System Components
-----------------

.. mermaid::

   flowchart LR
       UI["Flask Frontend<br/>(/)"] --> Views["automationv3.jobqueue.views"]
       Views --> Queue["JobQueue<br/>(SQLite)"]
       Views --> Central["CentralServer<br/>(/api/central)"]
       Views --> UUT["UUTStore"]
       Views --> Suites["SuiteManager"]
       Worker["WorkerServer<br/>jobqueue-worker"] --> Central
       Central --> Queue
       Central --> Results["Artifacts + Results"]

Job Flow
--------

.. mermaid::

   sequenceDiagram
       participant User
       participant FE as Frontend
       participant Q as JobQueue
       participant C as CentralServer
       participant W as Worker

       User->>FE: Queue script/job
       FE->>Q: add_job(...)
       C->>Q: get_next_job()
       C->>W: POST /jobs
       loop best-effort live stream
           W-->>C: POST /workers/<id>/events
       end
       W-->>C: POST /workers/<id>/result
       C->>Q: record_result(...)
       C->>Q: remove_job(...)
       loop guaranteed artifact sync
           C->>W: GET /artifacts/<job_id>/manifest
           C->>W: GET /artifacts/<job_id>/<path> (as needed)
           C->>C: verify worker/central tree hash parity via fscache
       end
       FE->>Q: list_results()

Execution and Reporting Details
-------------------------------

For a detailed breakdown of script chunking, block-level ``rvt-result`` output,
observer event streaming, and central/worker report data flow, see
:doc:`execution_reporting`.
