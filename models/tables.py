from sqlalchemy import (
    Column, Numeric, Integer, String, Text, Index, DateTime
)
from sqlalchemy.orm import DeclarativeBase
from datetime import timezone, datetime


class Base(DeclarativeBase):
    pass 

class OrderMetrics(Base):
    """
    Aggregated order metrics per 30-second tumbling window
    per product category.

    Granularity: one row per window_start x product_category.

    This is the Gold layer - pre-computed metrics ready for dashboard
    consumption. Raw order events live in kafka for the retention period
    (7 days) and are not stored in PostgreSQL.
    """
    __tablename__ = "order_metrics"

    # composite primary key 
    # uniquely identifies one metrics row
    # upsert or rerun updates in place
    window_start = Column(DateTime, primary_key=True)
    product_category = Column(String(100), primary_key=True)

    # volume metrics 
    total_orders = Column(Integer, nullable=False)
    total_quantity = Column(Integer, nullable=False)
    unique_products = Column(Integer, nullable=False)

    # revenue metrics
    total_revenue = Column(Numeric(15, 2), nullable=False)
    avg_order_value = Column(Numeric(10, 2), nullable=False)

    # dimension breakdowns
    # top continent by order volume in this window/category
    top_continent = Column(String(50), nullable=True)
    # gender with the highest order count in this window/category
    top_gender = Column(String(20), nullable=True)

    # window metadata
    window_end = Column(DateTime, nullable=False)
    window_seconds = Column(Integer, nullable=False, default=30)

    # audit_columns
    loaded_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("window_start_idx", "window_start"),
        Index("product_category_idx", "product_category")
    )
    