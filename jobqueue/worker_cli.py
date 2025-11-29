"""
Minimal worker startup script using docopt.

Usage:
  jobqueue-worker --central-url=<url> [--host=<host>] [--port=<port>] [--meta=<json>]

Options:
  --central-url=<url>   Base URL of central server (include prefix if used, e.g. http://localhost:5000/api/central)
  --host=<host>         Host to bind the worker HTTP server [default: 0.0.0.0]
  --port=<port>         Port to bind the worker HTTP server [default: 6000]
  --meta=<json>         JSON string of metadata to register with central (e.g. '{"name":"worker-1"}')
"""

import json
import logging
from typing import Dict

from docopt import docopt

from .worker_system import create_worker_app


def simple_job_runner(job: Dict) -> Dict:
    """Default job runner that echoes the received job; replace with real work."""
    return {"received": job}


def main() -> None:
    args = docopt(__doc__)
    central_url: str = args["--central-url"]
    host: str = args["--host"]
    port = int(args["--port"])

    meta: Dict = {}
    if args.get("--meta"):
        try:
            meta = json.loads(args["--meta"])
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON for --meta: {exc}") from exc

    worker_address = f"http://{host}:{port}"
    app = create_worker_app(
        central_url=central_url,
        worker_address=worker_address,
        job_runner=simple_job_runner,
        meta=meta,
    )

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting worker at %s (central: %s)", worker_address, central_url)
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()
