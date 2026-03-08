import json
import time
import logging 
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd
import yaml 
from pathlib import Path 
from dotenv import load_dotenv
import os
from src.aggregator import aggregate
from src.load import upsert_metrics
load_dotenv()
logger = logging.getLogger(__name__)

# Load config
config_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

topic = config["consumer"]["topic"]
bootstrap_servers = config['consumer']['bootstrap_servers']
group_id = config["consumer"]["group_id"]
window_seconds = config["consumer"]["window_seconds"]
poll_timeout_ms = config["consumer"]["poll_timeout_ms"]
auto_offset_reset = config["consumer"]["auto_offset_reset"]

def create_consumer() -> KafkaConsumer:
    """
    Creates and returns a KafkaConsumer instance.
    
    group_id - identifies this consumer as part of a consumer group. Kafka tracks offsets per group_id.
    If you run multiple consumer instances with the same group_id, Kafka distributes partitions across them
    automatically. With one instance, it reads all the partitions.
    
    auto_offset_reset="earliest" - if no committed offset exists for this group (first run), 
    start from the beginning of the topic. "latest" would skip all existing messages and only process new ones.
    For development "earliest" means you see all messages including ones sent before the consumer started.
    
    enable_auto_commit=False - we commit offsets manually after each successful flush. Auto-commit would commit 
    on a timer regardless of whether the database write succeeded - which violates the commit-after-flush guarantee.
    """
    consumer = KafkaConsumer(
        topic, bootstrap_servers=bootstrap_servers, group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        max_poll_records=100
    )
    return consumer 


def flush_window(
        window_events: list, window_start: datetime, engine
) -> int:
    """
    Converts accumulated window events to dataframe, computes
    metrics, writes to PostgreSQL and returns row count.
    
    Separated from the main loop so it can becalled both on timer 
    expiry and on clean shutdown.
    """
    if not window_events:
        logger.info(f"Window expired with no events - skipping flush")
        return 0
    
    df = pd.DataFrame(window_events)
    metrics_df = aggregate(df, window_start)

    if metrics_df.empty:
        logger.warning(f"Aggregation produced empty dataframe - skipping load")
        return 0
    
    rows_written = upsert_metrics(metrics_df, engine)

    logger.info(
        f"Window flushed - "
        f"events: {len(window_events)} | "
        f"metric rows: {rows_written} | "
        f"window: {window_start.strftime('%H:%M:%S')}"
    )
    return rows_written

def run_consumer(engine) -> None:
    """
    Runs the consumer loop indefinitely.

    Maintains a 30-second tumbling window in memory
    On window expiry: flush to PostgreSQL, commit offset, reset
    On shutdown: flush currrent partial window before closing.

    Why engine is passed as parameter rather than created here: Allows
    main.py to create one engine and share it across components. Creating
    multiple engines wastes connections.

    Args:
        engine: SQLAlchemy engine from load.get_engine()
    """
    logger.info(
        f"Starting consumer - "
        f"topic: {topic} | "
        f"group: {group_id} | "
        f"window: {window_seconds}s"
    )

    consumer = create_consumer()

    # window state 
    window_events = []
    window_start = datetime.now(timezone.utc)
    total_processed = 0
    total_flushed = 0

    try: 
        while True:
            # Poll for messages - blocks up to poll_timeout_ms
            # retruns dict of {TopicPartition: [messages]}
            message_batch = consumer.poll(
                timeout_ms=poll_timeout_ms
            )

            # process each message in the batch 
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    # message.value already deserialized to dict
                    # by value_deserializer in KafkaConsumer
                    event = message.value

                    if event is None:
                        logger.warning(
                            f"Received null message at "
                            f"offset {message.offset} - skipping"
                        )
                        continue 

                    window_events.append(event)
                    total_processed += 1

            # window expiry check
            # runs after every poll - not just when messages arrive
            # this ensures windows flush even during low traffic
            elapsed = (
                datetime.now(timezone.utc) - window_start
            ).total_seconds()

            if elapsed >= window_seconds:
                # step 1: flush to postgresql
                rows = flush_window(
                    window_events, window_start, engine
                )
                total_flushed += rows 

                # step 2: commit offset after successful flush 
                # if flush fails, exception propagates up, offset is not
                # committed, messages reprocessed on restart
                consumer.commit()

                # step 3: reset window state
                window_events = []
                window_start = datetime.now(timezone.utc)

            logger.info(
                f"Consumer heartbeat - "
                f"total processed: {total_processed} | "
                f"total flushed: {total_flushed}"
            )

    except KeyboardInterrupt:
        logger.info(f"Shutdown signal - flushing partial window")

        # flush whatever accumulated in the current partial window
        # Don't lose data just because the operator pressed Crtl+C
        if window_events:
            flush_window(window_events, window_start, engine)
            consumer.commit()

        logger.info(
            "Consumer stopped - "
            f"total processed: {total_processed} | "
            f"total flushed: {total_flushed}"
        )

    except Exception as e:
        logger.error(
            f"Unexpected consumer error: {type(e).__name__}: {e}", exc_info=True
        )
        raise 

    finally:
        consumer.close()
        logger.info(f"Kafka consumer connection closed")