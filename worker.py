"""Consume order events with manual acknowledgements and shared deduplication."""

import argparse
import fcntl
import json
import logging
import os
from pathlib import Path
import socket
import sys

import pika


LOGGER = logging.getLogger("order-worker")
DEFAULT_STATE_FILE = "/tmp/cmpe273-orders-state.json"


class SimulatedWorkerCrash(Exception):
    """Raised to close the connection with the current delivery unacknowledged."""


def load_state(state_path: Path) -> dict[str, object]:
    if not state_path.exists():
        return {"processed_event_ids": [], "crash_injection_used": False}
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read worker state file {state_path}: {exc}") from exc
    if not isinstance(state, dict):
        raise RuntimeError(f"Worker state file {state_path} must contain a JSON object")
    state.setdefault("processed_event_ids", [])
    state.setdefault("crash_injection_used", False)
    return state


def record_event(
    event_id: str, state_path: Path, inject_failure: bool
) -> tuple[bool, bool]:
    """Atomically record an event; return (is_duplicate, should_crash_before_ack)."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = state_path.with_suffix(state_path.suffix + ".lock")

    with lock_path.open("a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        state = load_state(state_path)
        processed = state["processed_event_ids"]
        if not isinstance(processed, list):
            raise RuntimeError("processed_event_ids in worker state must be a list")
        if event_id in processed:
            return True, False

        processed.append(event_id)
        should_crash = inject_failure and not bool(state["crash_injection_used"])
        if should_crash:
            state["crash_injection_used"] = True
        temporary_path = state_path.with_suffix(state_path.suffix + ".tmp")
        temporary_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary_path, state_path)
        return False, should_crash


def process_order(message: dict[str, object], worker_name: str) -> None:
    LOGGER.info(
        "PROCESSING worker=%s event_id=%s order_id=%s item_id=%s quantity=%s",
        worker_name,
        message["event_id"],
        message["order_id"],
        message["item_id"],
        message["quantity"],
    )


def valid_order(message: object) -> bool:
    return (
        isinstance(message, dict)
        and all(isinstance(message.get(key), str) and message[key] for key in ("event_id", "order_id", "item_id"))
        and isinstance(message.get("quantity"), int)
        and not isinstance(message.get("quantity"), bool)
        and message["quantity"] > 0
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--crash-before-ack-once",
        action="store_true",
        help="simulate one failure after processing but before ACK (shared across workers)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    worker_name = f"{socket.gethostname()}:{os.getpid()}"
    state_path = Path(os.getenv("ORDER_STATE_FILE", DEFAULT_STATE_FILE))
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    queue_name = os.getenv("QUEUE_NAME", "orders")

    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)

    def on_message(
        current_channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        del properties
        LOGGER.info("RECEIVED worker=%s delivery_tag=%s", worker_name, method.delivery_tag)
        try:
            message = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            LOGGER.exception("INVALID JSON; rejecting delivery without requeue")
            current_channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        if not valid_order(message):
            LOGGER.error("INVALID ORDER payload=%r; rejecting delivery without requeue", message)
            current_channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        is_duplicate, should_crash = record_event(
            str(message["event_id"]), state_path, args.crash_before_ack_once
        )
        if is_duplicate:
            LOGGER.warning(
                "DUPLICATE detected worker=%s event_id=%s; business action skipped",
                worker_name,
                message["event_id"],
            )
        else:
            process_order(message, worker_name)

        if should_crash:
            LOGGER.error(
                "SIMULATED FAILURE worker=%s event_id=%s; stopping before ACK (delivery will be requeued)",
                worker_name,
                message["event_id"],
            )
            raise SimulatedWorkerCrash

        current_channel.basic_ack(delivery_tag=method.delivery_tag)
        LOGGER.info(
            "ACK worker=%s event_id=%s",
            worker_name,
            message["event_id"],
        )

    channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)
    LOGGER.info(
        "STARTED worker=%s queue=%s state_file=%s",
        worker_name,
        queue_name,
        state_path,
    )
    try:
        channel.start_consuming()
    except SimulatedWorkerCrash:
        LOGGER.error("WORKER EXIT worker=%s; unacknowledged message will be redelivered", worker_name)
        return 2
    except KeyboardInterrupt:
        LOGGER.info("Stopping worker=%s", worker_name)
    finally:
        if connection.is_open:
            connection.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())