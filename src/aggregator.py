import pandas as pd
import logging 
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def get_top_value(series: pd.Series) -> str:
    """
    Returns the most frequently occuring value in a Series.
    Used for top_continent and top_gender - modal aggregations
    that don't fit standard agg() functions.
    
    Returns "Unknown" if series is empty to avoid index errors.
    """
    if series.empty:
        return "Unknown"
    
    return series.value_counts().index[0]

def aggregate(df: pd.DataFrame, window_start: datetime) -> pd.DataFrame:
    """
    Computes order metrics per product category for one 30-second tumbling window.
    
    Args:
        df: DataFrame of raw order events for this window
        window_start: when this window opened - becomes primary key
        
    Returns:
        DataFrame with one row per product_category containing all computed
        metrics. Empty DataFrame if input is empty.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to aggregator")
        return pd.DataFrame()

    logger.info(
        f"Aggregating {len(df)} events across "
        f"{df['product_category'].nunique()} categories"
    )

    # standard aggregations
    # One row per product_category
    # Each metric maps directly to one column in order_metrics table
    metrics_df = (
        df.groupby('product_category')
        .agg(
            total_orders = ("order_id", "count"),
            total_quantity = ("quantity", "sum"),
            unique_products = ("product_id", "nunique"),
            total_revenue = ("total_price", "sum"),
            avg_order_value = ("total_price", "mean")
        )
    ).reset_index()

    # Modal aggregations
    # top_continent and top_gender require value_counts
    # computed per category separately
    top_continent = (
        df.groupby("product_category")["continent"]
        .apply(get_top_value).reset_index()
        .rename(columns={"continent" : "top_continent"})
    )

    top_gender = (
        df.groupby("product_category")["gender"]
        .apply(get_top_value)
        .reset_index()
        .rename(columns={"gender" : "top_gender"})
    )

    # merge all metrics together
    metrics_df = metrics_df.merge(top_continent, on="product_category")
    metrics_df = metrics_df.merge(top_gender, on="product_category")

    # add window metadata
    window_end = window_start + pd.Timedelta(seconds=30)

    metrics_df["window_start"] = window_start
    metrics_df["window_end"] = window_end
    metrics_df["window_seconds"] = 30
    metrics_df["loaded_at"] = datetime.now(timezone.utc)

    # round numeric columns
    metrics_df["total_revenue"] = metrics_df["total_revenue"].round(2)
    metrics_df["avg_order_value"] = metrics_df["avg_order_value"].round(2)

    # reorder columns to match table schema
    final_columns = [
        "window_start",
        "product_category",
        "total_orders",
        "total_quantity",
        "unique_products",
        "total_revenue",
        "avg_order_value",
        "top_continent",
        "top_gender",
        "window_end",
        "window_seconds",
        "loaded_at"
    ]
    metrics_df = metrics_df[final_columns]

    logger.info(
        f"Aggregation complete - "
        f"{len(metrics_df)} category rows | "
        f"total orders: {metrics_df['total_orders'].sum()} | "
        f"total revenue: GHC {metrics_df['total_revenue'].sum():,.2f}"
    )

    return metrics_df                                   