"""Publish sample order events to RabbitMQ."""

import argparse
import json
import os
from uuid import uuid4

import pika


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=5, help="number of orders to publish")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")

    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    queue_name = os.getenv("QUEUE_NAME", "orders")
    parameters = pika.URLParameters(rabbitmq_url)

    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)

        for index in range(args.count):
            message = {
                "event_id": str(uuid4()),
                "order_id": str(uuid4()),
                "item_id": f"item-{(index % 3) + 1}",
                "quantity": (index % 5) + 1,
            }
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=json.dumps(message).encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
                mandatory=True,
            )
            print(f"PUBLISHED {json.dumps(message, sort_keys=True)}", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()