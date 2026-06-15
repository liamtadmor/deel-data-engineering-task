import json
import os
import time
import psycopg2
from datetime import date, timedelta
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
DB_URL = os.getenv("ANALYTICAL_DB_URL", "postgresql://finance_db_user:1234@transactions-db:5432/finance_db")

TOPICS = [
    'finance_db.operations.customers',
    'finance_db.operations.products',
    'finance_db.operations.orders',
    'finance_db.operations.order_items',
]


def epoch_days_to_yyyymmdd(epoch_days):
    return int((date(1970, 1, 1) + timedelta(days=epoch_days)).strftime('%Y%m%d'))


def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError:
            print("DB not ready, retrying in 3s...")
            time.sleep(3)


def init_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'analytics_pipeline_{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    while True:
        try:
            c = Consumer(conf)
            c.subscribe(TOPICS)
            return c
        except Exception as e:
            print(f"Kafka not ready: {e}, retrying in 5s...")
            time.sleep(5)


def get_or_create_customer(cursor, customer_id):
    cursor.execute(
        "SELECT customer FROM analytics.dim_customers WHERE customer_id = %s AND is_current = TRUE",
        (customer_id,)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "SELECT customer_name, customer_address FROM operations.customers WHERE customer_id = %s",
        (customer_id,)
    )
    src = cursor.fetchone()
    if not src:
        return None
    cursor.execute(
        """INSERT INTO analytics.dim_customers (customer_id, customer_name, customer_address, valid_from, is_current)
           VALUES (%s, %s, %s, NOW(), TRUE) RETURNING customer""",
        (customer_id, src[0], src[1])
    )
    return cursor.fetchone()[0]


def get_or_create_product(cursor, product_id):
    cursor.execute(
        "SELECT product FROM analytics.dim_products WHERE product_id = %s AND is_current = TRUE",
        (product_id,)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "SELECT product_name, barcode FROM operations.products WHERE product_id = %s",
        (product_id,)
    )
    src = cursor.fetchone()
    if not src:
        return None
    cursor.execute(
        """INSERT INTO analytics.dim_products (product_id, product_name, barcode, valid_from, is_current)
           VALUES (%s, %s, %s, NOW(), TRUE) RETURNING product""",
        (product_id, src[0], src[1])
    )
    return cursor.fetchone()[0]


def merge_into_fact(cursor, order_id, order_item_id):
    """Join staging tables and upsert into the fact table whenever both sides exist."""
    cursor.execute(
        """SELECT o.customer_id, o.order_date, o.delivery_date, o.status,
                  i.product_id, i.quantity
           FROM analytics.staging_orders o
           JOIN analytics.staging_order_items i ON i.order_id = o.order_id
           WHERE o.order_id = %s AND i.order_item_id = %s""",
        (order_id, order_item_id)
    )
    row = cursor.fetchone()
    if not row:
        return  # other side not yet arrived — will be triggered when it comes in

    customer_id, order_date, delivery_date, status, product_id, quantity = row

    customer_surrogate = get_or_create_customer(cursor, customer_id)
    if customer_surrogate is None:
        print(f"Customer {customer_id} not found, skipping fact merge for item {order_item_id}")
        return

    product_surrogate = get_or_create_product(cursor, product_id)
    if product_surrogate is None:
        print(f"Product {product_id} not found, skipping fact merge for item {order_item_id}")
        return

    cursor.execute(
        "SELECT unity_price FROM operations.products WHERE product_id = %s",
        (product_id,)
    )
    price_row = cursor.fetchone()
    unity_price = float(price_row[0]) if price_row else 0.0
    total_amount = float(quantity) * unity_price

    cursor.execute(
        """INSERT INTO analytics.customer_order_items
               (order_id, order_item_id, customer, product, order_date, delivery_date,
                status, quantity, unity_price, total_amount, updated_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
           ON CONFLICT (order_id, order_item_id) DO UPDATE
               SET status        = EXCLUDED.status,
                   quantity      = EXCLUDED.quantity,
                   total_amount  = EXCLUDED.total_amount,
                   delivery_date = EXCLUDED.delivery_date,
                   updated_at    = NOW()""",
        (order_id, order_item_id, customer_surrogate, product_surrogate,
         order_date, delivery_date, status, quantity, unity_price, total_amount)
    )
    print(f"Fact upserted: order={order_id} item={order_item_id} status={status}")


def handle_customer(cursor, after):
    customer_id = after['customer_id']
    cursor.execute(
        "UPDATE analytics.dim_customers SET is_current = FALSE, valid_to = NOW() WHERE customer_id = %s AND is_current = TRUE",
        (customer_id,)
    )
    cursor.execute(
        """INSERT INTO analytics.dim_customers (customer_id, customer_name, customer_address, valid_from, is_current)
           VALUES (%s, %s, %s, NOW(), TRUE)""",
        (customer_id, after['customer_name'], after['customer_address'])
    )
    print(f"Customer {customer_id} upserted")


def handle_product(cursor, after):
    product_id = after['product_id']
    cursor.execute(
        "UPDATE analytics.dim_products SET is_current = FALSE, valid_to = NOW() WHERE product_id = %s AND is_current = TRUE",
        (product_id,)
    )
    cursor.execute(
        """INSERT INTO analytics.dim_products (product_id, product_name, barcode, valid_from, is_current)
           VALUES (%s, %s, %s, NOW(), TRUE)""",
        (product_id, after['product_name'], after['barcode'])
    )
    print(f"Product {product_id} upserted")


def handle_order(cursor, after):
    order_id = after['order_id']
    status = after['status']
    raw_dd = after.get('delivery_date')
    delivery_date = epoch_days_to_yyyymmdd(raw_dd) if raw_dd else None
    raw_od = after.get('order_date')
    order_date = epoch_days_to_yyyymmdd(raw_od) if raw_od else int(time.strftime('%Y%m%d'))

    # Upsert into staging — captures every status change
    cursor.execute(
        """INSERT INTO analytics.staging_orders (order_id, customer_id, order_date, delivery_date, status, updated_at)
           VALUES (%s, %s, %s, %s, %s, NOW())
           ON CONFLICT (order_id) DO UPDATE
               SET status = EXCLUDED.status,
                   delivery_date = EXCLUDED.delivery_date,
                   updated_at = NOW()""",
        (order_id, after['customer_id'], order_date, delivery_date, status)
    )
    print(f"Order {order_id} staged: status={status}")

    # Update any fact rows already inserted for this order
    cursor.execute(
        """UPDATE analytics.customer_order_items
           SET status = %s, delivery_date = COALESCE(%s, delivery_date), updated_at = NOW()
           WHERE order_id = %s""",
        (status, delivery_date, order_id)
    )

    # Merge any order_items that arrived before this order event
    cursor.execute(
        "SELECT order_item_id FROM analytics.staging_order_items WHERE order_id = %s",
        (order_id,)
    )
    for (order_item_id,) in cursor.fetchall():
        merge_into_fact(cursor, order_id, order_item_id)


def handle_order_item(cursor, after):
    order_item_id = after['order_item_id']
    order_id = after['order_id']
    product_id = after['product_id']
    quantity = after['quanity']  # typo in source schema

    # Upsert into staging
    cursor.execute(
        """INSERT INTO analytics.staging_order_items (order_item_id, order_id, product_id, quantity, updated_at)
           VALUES (%s, %s, %s, %s, NOW())
           ON CONFLICT (order_item_id) DO UPDATE
               SET quantity = EXCLUDED.quantity,
                   updated_at = NOW()""",
        (order_item_id, order_id, product_id, quantity)
    )
    print(f"Order item {order_item_id} staged")

    # Merge — will succeed if the orders event already arrived, otherwise waits
    merge_into_fact(cursor, order_id, order_item_id)


def process_stream():
    conn = get_db_connection()
    cursor = conn.cursor()
    consumer = init_consumer()

    print("Streaming pipeline active, listening for CDC events...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                print(f"Failed to parse message: {e}")
                continue

            after = payload.get('after')
            if not after:
                continue

            topic = msg.topic()

            try:
                if 'customers' in topic:
                    handle_customer(cursor, after)
                elif 'products' in topic:
                    handle_product(cursor, after)
                elif 'order_items' in topic:
                    handle_order_item(cursor, after)
                elif 'orders' in topic:
                    handle_order(cursor, after)

                conn.commit()

            except Exception as e:
                print(f"Error processing {topic}: {e}")
                conn.rollback()

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    process_stream()
