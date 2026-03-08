import json
import time 
import uuid 
import logging
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
import yaml
from pathlib import Path

load_dotenv()
logger = logging.getLogger(__name__)
fake = Faker()

# Load Config
config_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(config_path) as file:
    config = yaml.safe_load(file)

topic = config["producer"]["topic"]
bootstrap_servers = config["producer"]["bootstrap_servers"]
send_interval = config["producer"]["send_interval_seconds"]
log_every = config["producer"]["log_every_n_events"]
categories = config["producer"]["product_categories"]
continents = config["producer"]["continents"]

def create_producer() -> KafkaProducer:
    """
    Creates and reruns a KafkaProducer instance.
    
    value_serializer converts Python dict -> JSON string -> bytes
    automatically on every send. This means send() accepts a dict
    directly - serialization is handled transparently.
    
    key_serializer converts the partition key string -> bytes.
    Kafka requires bytes for both key and value.
    
    Why acks="all"?
    The producer waits for acknowledgement from all in-sync
    replicas before considering a send successful. In development with one
    broker this makes no difference. In production it prevents data loss
    if the leader broker fails immediately after receiving a message
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers, 
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=500
    )


def generate_order_event() -> dict:
    """
    Generates one realistic order event using Faker.
    
    product_category comes from a fixed list - random Faker
    output would produce hundreds of unique categories making
    aggregation meaningless.
    
    order_timestamp uses ISO8601 format - consumers parse this to 
    determine window assignment. UTC ensures consistent window
    boundaries regardless of server timezone.
    """
    category = random.choice(categories)
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(5.0, 500.0), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "product_id": f"PROD-{random.randint(1000, 9999)}",
        "product_name": fake.bs().title()[:50],
        "product_category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": round(quantity * unit_price, 2),
        "continent": random.choice(continents),
        "gender": random.choice(["Male","Female","Other"])
    }

def on_send_sucess(record_metdata) -> None:
    """
    Callback fired when a message is successfully acknowledged by Kafka.
    Log partition and offset for debugging partition distribution
    """
    logger.debug(
        f"Message delivered -> "
        f"topic={record_metdata.topic} "
        f"partition={record_metdata.partition} "
        f"offset={record_metdata.offset}"
    )

def on_send_error(exception) -> None:
    """
    Callback fired when a message fails after all retries.
    Logs the error but does not crash the producer loop - 
    one failed message should not stop the entire stream
    """
    logger.error(f"Message delivery failed: {exception}")

def run_producer() -> None:
    """
    Runs the producer loop indefinitely.
    
    Generates one order event per SEND_INTERVAL seconds,
    sends to kafka topic with product_category as partition key
    
    Logs a summary every LOG_EVERY events showing distribution
    across categories - useful for verifying partition load.
    
    Shuts down cleanly on KeyboardInterrupt - flushes any buffered
    messages before closing the connection
    """
    logger.info(f"Starting producer -> topic: {topic}")
    logger.info(f"Send interval: {send_interval} | categories: {categories}")

    producer = create_producer()

    events_sent = 0
    category_count = {cat: 0 for cat in categories}
    failed_sends = 0

    try:
        while True:
            event = generate_order_event()
            category = event["product_category"]

            producer.send(
                topic=topic,
                key=category,
                value=event
            ).add_callback(on_send_sucess).add_errback(on_send_error)

            events_sent += 1
            category_count[category] += 1

            # periodic summary - LOG EVERY events
            if events_sent % log_every == 0:
                logger.info(
                    f"Producer summary - "
                    f"total sent: {events_sent} | "
                    f"failed: {failed_sends} | "
                    f"distribution: {category_count}"
                )

            time.sleep(send_interval)
    
    except KeyboardInterrupt:
        logger.info("Shutdown signal received - flushing buffered messages")
        producer.flush()
        producer.close()
        logger.info(
            f"Producer stopped - "
            f"total events sent: {events_sent} | "
            f"failed: {failed_sends}"
        )

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    run_producer()