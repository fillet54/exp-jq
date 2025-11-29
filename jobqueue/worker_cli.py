"""
Minimal worker startup script using docopt.

Usage:
  jobqueue-worker --central-url=<url> [--host=<host>] [--port=<port>] [--meta=<json>] [--advertise-address=<addr>]

Options:
  --central-url=<url>   Base URL of central server (include prefix if used, e.g. http://localhost:5000/api/central)
  --host=<host>         Host to bind the worker HTTP server [default: 0.0.0.0]
  --port=<port>         Port to bind the worker HTTP server [default: 6000]
  --meta=<json>         JSON string of metadata to register with central (e.g. '{"name":"worker-1"}')
  --advertise-address=<addr>   Address central should use to reach this worker (default: derived from host/port; if host is 0.0.0.0 this becomes http://127.0.0.1:<port>)
"""

import json
import logging
from typing import Dict

from docopt import docopt

from .executor import run_job
from .worker_system import create_worker_app


def main() -> None:
    args = docopt(__doc__)
    central_url: str = args["--central-url"]
    host: str = args["--host"]
    port = int(args["--port"])
    advertise = args.get("--advertise-address")

    meta: Dict = {}
    if args.get("--meta"):
        try:
            meta = json.loads(args["--meta"])
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON for --meta: {exc}") from exc

    # When binding to 0.0.0.0, use localhost for the advertised address so HTTP checks succeed on the same machine.
    if advertise:
        worker_address = advertise
    else:
        address_host = "127.0.0.1" if host == "0.0.0.0" else host
        worker_address = f"http://{address_host}:{port}"
    app = create_worker_app(
        central_url=central_url,
        worker_address=worker_address,
        job_runner=run_job,
        meta=meta,
    )

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting worker at %s (central: %s)", worker_address, central_url)
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()
