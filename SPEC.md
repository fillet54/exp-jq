# ✅ Job Queue Specification

## 🧱 Overview

This is a **SQLite-backed job queue** system for scheduling automation scripts. It supports:

- Persistent, ordered job queue
- Unique job identifiers (UUID)
- Job prioritization
- Skipping/restoring jobs
- Promoting jobs to the top
- Batch or single job insertion
- External decision-making (queue just returns jobs, user decides what to do)

---

## 🗂️ Job Structure

Each job is a dictionary with the following minimum keys:

```python
{
    "file": "scripts/test1.py",      # Path to the script
    "uut": "UUT1",                   # Unit under test
    "report_id": "R123"              # Report identifier
}
```

The queue adds:

- `job_id`: auto-generated UUIDv4
- `priority`: integer (default = 0; higher is prioritized)
- `inserted_at`: timestamp (float)
- `skipped`: boolean flag (0/1 in DB)

---

## 📁 Database Schema (SQLite)

```sql
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    job_data TEXT,
    skipped INTEGER DEFAULT 0,
    priority INTEGER DEFAULT 0,
    inserted_at REAL
);
```

---

## ⚙️ JobQueue API

### `add_job(job_or_jobs: dict | list, priority: int = 0) -> str | list`
Add a single job or list of jobs.

- Uses batch insert (`executemany`) if input is a list
- Returns:
  - Single `job_id` if input is one job
  - List of `job_id`s if multiple

---

### `get_next_job() -> dict | None`
Returns the next available job (not skipped), ordered by:

1. `priority DESC`
2. `inserted_at ASC`

Returns `None` if the queue is empty or all jobs are skipped.

---

### `mark_skipped(job_id: str)`
Marks the specified job as skipped (temporarily hidden from `get_next_job()`).

---

### `restore_all_skipped()`
Restores all skipped jobs (sets `skipped = 0` for all jobs).

---

### `remove_job(job_id: str)`
Removes a job completely from the queue.

---

### `promote_job(job_id: str, new_priority: int = 100)`
Promotes a job to the front of the queue by:

- Setting `priority = new_priority`
- Updating `inserted_at = current_time`

---

### `list_jobs() -> list[dict]`
Returns all jobs (in priority + FIFO order), each as:

```python
{
    ...job_data,
    'job_id': str,
    'priority': int,
    'inserted_at': float,
    'skipped': bool
}
```

---

## 🧪 Sample Usage Pattern

```python
queue = JobQueue()

# Add a batch of jobs
queue.add_job([
    {"file": "a.py", "uut": "UUT1", "report_id": "R1"},
    {"file": "b.py", "uut": "UUT2", "report_id": "R2"}
], priority=5)

# Get the next job
job = queue.get_next_job()
if job:
    if is_worker_compatible(job):
        run_job(job)
        queue.remove_job(job['job_id'])
    else:
        queue.mark_skipped(job['job_id'])

# If queue is empty or blocked, restore skipped
if not queue.get_next_job():
    queue.restore_all_skipped()
```

---

## 🔚 Optional Enhancements

- Add job status tracking (`pending`, `running`, `done`, `failed`)
- Add support for job expiration or retry counters
- Wrap this logic in a Flask API for remote job management