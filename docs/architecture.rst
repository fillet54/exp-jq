Architecture
============

System Components
-----------------

.. mermaid::

   flowchart LR
       UI["Flask Frontend<br/>(/)"] --> Views["jobqueue.views"]
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
       W-->>C: POST /workers/<id>/result
       C->>Q: record_result(...)
       C->>Q: remove_job(...)
       FE->>Q: list_results()
