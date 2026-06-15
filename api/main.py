import os
from typing import Optional
from datetime import date
from fastapi import FastAPI, HTTPException, Query
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager

app = FastAPI(
    title="Acme Financial Analytics Real-Time API",
    description="Live reporting endpoints fed by CDC Debezium data streams",
    version="1.0.0"
)

# Configuration from environment variables
DB_URL = os.getenv("ANALYTICAL_DB_URL", "postgresql://finance_db_user:1234@transactions-db:5432/finance_db")

# Initialize a thread-safe connection pool for high-concurrency API performance
try:
    db_pool = SimpleConnectionPool(1, 20, dsn=DB_URL)
except Exception as e:
    print(f"Failed to initialize database connection pool: {e}")
    raise e

@contextmanager
def get_db_cursor():
    """Context manager to safely acquire and release database connections from the pool."""
    conn = db_pool.getconn()
    try:
        yield conn.cursor()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        db_pool.putconn(conn)


# 1. GET /analytics/orders?status=open
@app.get("/analytics/orders", tags=["Analytics Operations"])
def get_orders_by_delivery_date_and_status(
        status: str = Query("open", description="Filter by order status"),
        date_from: Optional[date] = Query(None, description="Start of delivery date range (inclusive)"),
        date_to: Optional[date] = Query(None, description="End of delivery date range (inclusive)"),
):
    """Retrieves the aggregate count of orders grouped by DELIVERY_DATE and STATUS."""
    filters = ["LOWER(status) = LOWER(%s)"]
    params: list = [status]
    if date_from:
        filters.append("delivery_date >= %s")
        params.append(date_from)
    if date_to:
        filters.append("delivery_date <= %s")
        params.append(date_to)
    where = " AND ".join(filters)
    query = f"""
            SELECT delivery_date, status, COUNT(DISTINCT order_id) as order_count
            FROM analytics.customer_order_items
            WHERE {where}
            GROUP BY delivery_date, status
            ORDER BY delivery_date DESC;
            """
    try:
        with get_db_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {"delivery_date": row[0], "status": row[1], "order_count": row[2]}
                for row in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database aggregation error: {str(e)}")


# 2. GET /analytics/orders/top?limit=3
@app.get("/analytics/orders/top", tags=["Analytics Operations"])
def get_top_delivery_dates(
        status: str = Query("open", description="Filter by order status"),
        limit: int = Query(3, ge=1, le=100, description="Number of top dates to return"),
        date_from: Optional[date] = Query(None, description="Start of delivery date range (inclusive)"),
        date_to: Optional[date] = Query(None, description="End of delivery date range (inclusive)"),
):
    """Retrieves the top N delivery dates with the highest volume of open/pending orders."""
    filters = ["LOWER(status) = LOWER(%s)", "delivery_date IS NOT NULL"]
    params: list = [status]
    if date_from:
        filters.append("delivery_date >= %s")
        params.append(date_from)
    if date_to:
        filters.append("delivery_date <= %s")
        params.append(date_to)
    params.append(limit)
    where = " AND ".join(filters)
    query = f"""
            SELECT delivery_date, COUNT(DISTINCT order_id) as open_order_count
            FROM analytics.customer_order_items
            WHERE {where}
            GROUP BY delivery_date
            ORDER BY open_order_count DESC
            LIMIT %s;
            """
    try:
        with get_db_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {"delivery_date": row[0], "open_order_count": row[1]}
                for row in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database aggregation error: {str(e)}")


# 3. GET /analytics/orders/product
@app.get("/analytics/orders/product", tags=["Analytics Operations"])
def get_open_items_by_product(
        status: str = Query("open", description="Filter by order status"),
        date_from: Optional[date] = Query(None, description="Start of delivery date range (inclusive)"),
        date_to: Optional[date] = Query(None, description="End of delivery date range (inclusive)"),
):
    """Retrieves total quantity of outstanding items currently pending/open, split by PRODUCT_ID."""
    filters = ["LOWER(status) = LOWER(%s)"]
    params: list = [status]
    if date_from:
        filters.append("delivery_date >= %s")
        params.append(date_from)
    if date_to:
        filters.append("delivery_date <= %s")
        params.append(date_to)
    where = " AND ".join(filters)
    query = f"""
            SELECT product as product_id, SUM(quantity) as total_pending_quantity
            FROM analytics.customer_order_items
            WHERE {where}
            GROUP BY product
            ORDER BY total_pending_quantity DESC;
            """
    try:
        with get_db_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {"product_id": row[0], "total_pending_quantity": int(row[1])}
                for row in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database aggregation error: {str(e)}")


# 4. GET /analytics/orders/customers/?status=open&limit=3
@app.get("/analytics/orders/customers/", tags=["Analytics Operations"])
def get_top_customers_with_pending_orders(
        status: str = Query("open", description="Filter by order status"),
        limit: int = Query(3, ge=1, le=100, description="Number of top customers to return"),
        date_from: Optional[date] = Query(None, description="Start of delivery date range (inclusive)"),
        date_to: Optional[date] = Query(None, description="End of delivery date range (inclusive)"),
):
    """Retrieves the top N customers with the highest volume of pending/open orders."""
    filters = ["LOWER(status) = LOWER(%s)"]
    params: list = [status]
    if date_from:
        filters.append("delivery_date >= %s")
        params.append(date_from)
    if date_to:
        filters.append("delivery_date <= %s")
        params.append(date_to)
    params.append(limit)
    where = " AND ".join(filters)
    query = f"""
            SELECT customer as customer_id, COUNT(DISTINCT order_id) as pending_order_count
            FROM analytics.customer_order_items
            WHERE {where}
            GROUP BY customer
            ORDER BY pending_order_count DESC
            LIMIT %s;
            """
    try:
        with get_db_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {"customer_id": row[0], "pending_order_count": row[1]}
                for row in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database aggregation error: {str(e)}")

@app.on_event("shutdown")
def shutdown_db_pool():
    """Gracefully closes all connections in the pool when the API turns off."""
    db_pool.closeall()