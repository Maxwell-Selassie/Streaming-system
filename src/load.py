import logging
import pandas as pd
from sqlalchemy import create_engine, text 
from sqlalchemy import Engine 
from dotenv import load_dotenv
import os 

load_dotenv() 
logger = logging.getLogger(__name__)

def upsert_metrics(
        df: pd.DataFrame,
        engine: Engine
) -> int: 
    """
    Upserts aggregated window metrics into order_metrics table.
    
    Idempotent - if the consumer reprocesses a window due to a crash
    and restarts, the same window_start x product_combination updates the existing row rather than
    inserting a duplicate.
    
    This is the at-least-once delivery safety net - Kafka may deliver messages more than once, but the uspert
    ensures the metrics table always reflects the correct final state.
    
    Returns count of rows upserted.
    """
    if df.empty:
        logger.warning("Empty metrics DataFrame - nothing to upsert")
        return 0
    
    upsert_sql = """
        INSERT INTO order_metrics (
            window_start, product_category,
            total_orders, total_quantity, unique_products,
            total_revenue, avg_order_value,
            top_continent, top_gender,
            window_end, window_seconds, loaded_at
        ) VALUES (
            :window_start, :product_category,
            :total_orders, :total_quantity, :unique_products,
            :total_revenue, :avg_order_value,
            :top_continent, :top_gender,
            :window_end, :window_seconds, :loaded_at
        ) ON CONFLICT (window_start, product_category)
        DO UPDATE SET 
            total_orders    = EXCLUDED.total_orders,
            total_quantity  = EXCLUDED.total_quantity,
            unique_products = EXCLUDED.unique_products,
            total_revenue   = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value,
            top_continent   = EXCLUDED.top_continent,
            top_gender      = EXCLUDED.top_gender,
            window_end      = EXCLUDED.window_end,
            loaded_at       = EXCLUDED.loaded_at;
    """
    rows = df.to_dict(orient="records")

    with engine.connect() as conn:
        conn.execute(text(upsert_sql), rows)
        conn.commit()

    logger.info(
        f"Upserted {len(rows)} metric rows into order_metrics"
    )
    return len(rows)    