import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    # aio-pika / aiormq are chatty on reconnects; keep them at WARNING.
    for noisy in ("aiormq", "aio_pika"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
