SQL Schemas
===========

This page documents every SQLite schema currently created by the project.

Database Scope
--------------

All tables are created in the same SQLite database file (configured by
``JOBQUEUE_DB``, default ``jobqueue.db``). The schema is initialized by:

- ``automationv3/jobqueue/__init__.py`` (job/report/result tables)
- ``automationv3/jobqueue/uut.py`` (UUT configuration table)

Entity Relationship View
------------------------

.. mermaid::

   erDiagram
       reports ||--o{ jobs : contains
       reports ||--o{ job_results : records
       reports ||--o{ pending_results : buffers
       reports ||--o{ report_scripts : tracks
       reports ||--o{ report_requirements : covers

       jobs ||--o{ job_results : completes
       jobs ||--o{ pending_results : buffers

       reports {
           TEXT report_id PK
           TEXT title
           TEXT description
           REAL created_at
       }

       jobs {
           TEXT job_id PK
           TEXT report_id FK
           TEXT job_data
           INTEGER skipped
           INTEGER priority
           REAL inserted_at
       }

       job_results {
           TEXT job_id PK
           TEXT report_id FK
           TEXT job_data
           TEXT result_data
           INTEGER success
           TEXT worker_id
           TEXT worker_address
           REAL completed_at
           TEXT suite_run_id
           TEXT artifacts_manifest
           INTEGER artifacts_downloaded
       }

       pending_results {
           TEXT job_id PK
           TEXT report_id FK
           TEXT job_data
           TEXT result_data
           INTEGER success
           TEXT worker_id
           TEXT worker_address
           REAL received_at
           TEXT artifacts_manifest
           INTEGER sync_attempts
           TEXT last_error
       }

       report_scripts {
           TEXT report_id PK
           TEXT script_path PK
           TEXT job_template
           REAL created_at
           REAL updated_at
       }

       report_requirements {
           TEXT report_id PK
           TEXT requirement_id PK
           REAL created_at
           REAL updated_at
       }

       uut_configs {
           TEXT uut_id PK
           TEXT name
           TEXT path
           TEXT last_tree_sha
           REAL updated_at
       }

Notes:

- ``report_id`` is a first-class relational key on ``jobs``, ``job_results``,
  and ``pending_results``.
- Foreign key checks are enforced per connection using
  ``PRAGMA foreign_keys = ON``.
- ``job_results.job_id`` and ``pending_results.job_id`` remain logical links to
  queued ``jobs.job_id`` values, because completed/pending rows can outlive
  queue rows.
- Several fields store JSON payloads as TEXT (for example ``job_data``,
  ``result_data``, ``artifacts_manifest``, ``job_template``).

Table Reference
---------------

``jobs``
~~~~~~~~

Source: ``automationv3/jobqueue/__init__.py``

- ``job_id TEXT PRIMARY KEY``
- ``report_id TEXT NOT NULL`` (FK to ``reports.report_id``, delete cascade)
- ``job_data TEXT`` (JSON job payload snapshot)
- ``skipped INTEGER DEFAULT 0`` (boolean-ish 0/1)
- ``priority INTEGER DEFAULT 0``
- ``inserted_at REAL`` (epoch seconds)

``job_results``
~~~~~~~~~~~~~~~

Source: ``automationv3/jobqueue/__init__.py``

- ``job_id TEXT PRIMARY KEY``
- ``report_id TEXT NOT NULL`` (FK to ``reports.report_id``, delete cascade)
- ``job_data TEXT`` (JSON snapshot of job at completion)
- ``result_data TEXT`` (JSON result payload)
- ``success INTEGER`` (boolean-ish 0/1)
- ``worker_id TEXT``
- ``worker_address TEXT``
- ``completed_at REAL`` (epoch seconds)
- ``suite_run_id TEXT``
- ``artifacts_manifest TEXT`` (JSON list of artifact paths)
- ``artifacts_downloaded INTEGER DEFAULT 0`` (boolean-ish 0/1)

``reports``
~~~~~~~~~~~

Source: ``automationv3/jobqueue/__init__.py``

- ``report_id TEXT PRIMARY KEY``
- ``title TEXT NOT NULL``
- ``description TEXT``
- ``created_at REAL NOT NULL`` (epoch seconds)

``pending_results``
~~~~~~~~~~~~~~~~~~~

Source: ``automationv3/jobqueue/__init__.py``

- ``job_id TEXT PRIMARY KEY``
- ``report_id TEXT NOT NULL`` (FK to ``reports.report_id``, delete cascade)
- ``job_data TEXT`` (JSON snapshot)
- ``result_data TEXT`` (JSON payload)
- ``success INTEGER`` (boolean-ish 0/1)
- ``worker_id TEXT``
- ``worker_address TEXT``
- ``received_at REAL`` (epoch seconds)
- ``artifacts_manifest TEXT`` (JSON list)
- ``sync_attempts INTEGER DEFAULT 0``
- ``last_error TEXT``

``report_scripts``
~~~~~~~~~~~~~~~~~~

Source: ``automationv3/jobqueue/__init__.py``

- ``report_id TEXT NOT NULL``
- ``script_path TEXT NOT NULL``
- ``job_template TEXT`` (JSON template used to requeue)
- ``created_at REAL NOT NULL``
- ``updated_at REAL NOT NULL``
- ``FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE``
- ``PRIMARY KEY (report_id, script_path)``

``report_requirements``
~~~~~~~~~~~~~~~~~~~~~~~

Source: ``automationv3/jobqueue/__init__.py``

- ``report_id TEXT NOT NULL``
- ``requirement_id TEXT NOT NULL``
- ``created_at REAL NOT NULL``
- ``updated_at REAL NOT NULL``
- ``FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE``
- ``PRIMARY KEY (report_id, requirement_id)``

``uut_configs``
~~~~~~~~~~~~~~~

Source: ``automationv3/jobqueue/uut.py``

- ``uut_id TEXT PRIMARY KEY``
- ``name TEXT``
- ``path TEXT``
- ``last_tree_sha TEXT``
- ``updated_at REAL``

Schema Initialization
---------------------

The schema is defined directly in ``JobQueue._init_db`` and created with
``CREATE TABLE IF NOT EXISTS`` statements and supporting indexes.

This version assumes a clean database start rather than in-place legacy schema
migrations.
