"""Optional combined home-side runtime for the enqueuer and reconciler.

Existing deployments keep their separate services. Operators can select this
entry point to share imported modules while retaining independent clients,
threads, error handling, and polling cadences for the two roles.
"""

from __future__ import annotations

import argparse
import logging
import queue
import threading

from . import enqueuer, reconciler

log = logging.getLogger("node_relay.home")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="combined node-relay home runtime")
    ap.add_argument("--enqueuer-interval", type=float, default=15.0)
    ap.add_argument("--reconciler-interval", type=float, default=15.0)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    failures: queue.Queue[tuple[str, BaseException]] = queue.Queue()

    def run_role(name: str, target, interval: float) -> None:
        try:
            target(["--interval", str(interval)])
        except BaseException as exc:  # preserve role identity across the thread boundary
            failures.put((name, exc))

    threads = [
        threading.Thread(
            target=run_role,
            args=("enqueuer", enqueuer.main, args.enqueuer_interval),
            name="node-relay-enqueuer",
            daemon=True,
        ),
        threading.Thread(
            target=run_role,
            args=("reconciler", reconciler.main, args.reconciler_interval),
            name="node-relay-reconciler",
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    try:
        name, exc = failures.get()
    except KeyboardInterrupt:
        log.info("combined home runtime stopped")
        return
    raise RuntimeError(f"{name} role stopped unexpectedly") from exc


if __name__ == "__main__":
    main()
