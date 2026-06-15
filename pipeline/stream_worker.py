import json
import os
import sys
import time
import psycopg2
# 🚀 Switch to the high-performance Confluent Kafka engine
from confluent_kafka import Consumer, KafkaError

# Configuration from environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
DB_URL = os.getenv("ANALYTICAL_DB_URL", "postgresql://finance_db_user:1234@transactions-db:5432/finance_db")

DIM_DATE_TIMESTAMP_FORMAT = '%Y%m%d'

def get_db_connection():
    """Retries database connection if it is not immediately ready on startup."""
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            return conn
        except psycopg2.OperationalError:
            print("Analytics Database not ready yet, retrying in 3 seconds...")
            time.sleep(3)

def init_kafka_consumer():
    """Initializes Confluent Kafka consumer with automated topic subscriptions."""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'analytics_sync_workers_v3',  # Fresh consumer group to clear offsets
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

    topics = [
        'finance_db.operations.customers',
        'finance_db.operations.products',
        'finance_db.operations.orders',
        'finance_db.operations.order_items'
    ]

    while True:
        try:
            consumer = Consumer(conf)
            consumer.subscribe(topics)
            return consumer
        except Exception as e:
            print(f"Waiting for Kafka Broker ({KAFKA_BROKER}) to stabilize... Error: {e}")
            time.sleep(5)

def process_stream():
    conn = get_db_connection()
    cursor = conn.cursor()
    consumer = init_kafka_consumer()

    print("🚀 Streaming pipeline is active and listening for CDC changes...")

    try:
        while True:
            # Poll for a message (1.0-second timeout)
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka error: {msg.error()}")
                    continue

            # Deserialize JSON payload safely
            try:
                msg_payload = json.loads(msg.value().decode('utf-8'))
                print(msg_payload)
                # 🚀 DEBUG LOGGER: This will explicitly tell us if data is hitting the container!
                print(f"📥 RAW EVENT DETECTED on topic [{msg.topic()}]: {json.dumps(msg_payload)[:200]}...")
            except Exception as parse_err:
                print(f"Failed to parse JSON string: {parse_err}")
                continue

            after_state = msg_payload['after']
            if not after_state:
                continue

            topic = msg.topic()
            print(topic)

            try:
                # 1. HANDLE CUSTOMERS CHANGING (SCD TYPE 2)
                if 'customers' in topic:
                    customer_id = after_state['customer_id']
                    print(f"👤 Processing Customer {customer_id} update...")

                    cursor.execute(
                        "SELECT customer FROM analytics.dim_customers WHERE customer_id = %s AND is_current = TRUE",
                        (customer_id,)
                    )
                    exists = cursor.fetchone()

                    if exists:
                        cursor.execute("""
                                       UPDATE analytics.dim_customers
                                       SET is_current = FALSE, valid_to = NOW()
                                       WHERE customer_id = %s AND is_current = TRUE;
                                       """, (customer_id,))

                    cursor.execute("""
                                   INSERT INTO analytics.dim_customers (customer_id, customer_name, customer_address, valid_from, is_current)
                                   VALUES (%s, %s, %s, NOW(), TRUE);
                                   """, (customer_id, after_state['customer_name'], after_state['customer_address']))

                # 2. HANDLE PRODUCTS CHANGING (SCD TYPE 2)
                elif 'products' in topic:
                    product_id = after_state['product_id']
                    print(f"🏷️ Processing Product {product_id} update...")

                    cursor.execute(
                        "SELECT product FROM analytics.dim_products WHERE product_id = %s AND is_current = TRUE",
                        (product_id,)
                    )
                    exists = cursor.fetchone()

                    if exists:
                        cursor.execute("""
                                       UPDATE analytics.dim_products
                                       SET is_current = FALSE, valid_to = NOW()
                                       WHERE product_id = %s AND is_current = TRUE;
                                       """, (product_id,))

                    cursor.execute("""
                                   INSERT INTO analytics.dim_products (product_id, product_name, barcode, valid_from, is_current)
                                   VALUES (%s, %s, %s, NOW(), TRUE);
                                   """, (product_id, after_state['product_name'], after_state['barcode']))

                # 3. HANDLE LIVE ORDER ITEMS CHANGES (FACT UPSERT ENGINE)
                elif 'order_items' in topic:
                    order_item_id = after_state['order_item_id']
                    order_id = after_state['order_id']
                    product_id = after_state['product_id']
                    quantity = after_state['quanity']
                    print(f"📦 Processing Order Item {order_item_id} for Order {order_id}...")

                    cursor.execute(
                        "SELECT customer_id, order_date, delivery_date, status FROM operations.orders WHERE order_id = %s",
                        (order_id,)
                    )
                    order_header = cursor.fetchone()

                    if not order_header:
                        print(f"⚠️ Header for order {order_id} not available yet. Skipping item.")
                        continue

                    cust_id, order_date, delivery_date, status = order_header

                    order_date = int(order_date.strftime(DIM_DATE_TIMESTAMP_FORMAT)) if order_date else int(time.strftime(DIM_DATE_TIMESTAMP_FORMAT))
                    delivery_date = int(delivery_date.strftime(DIM_DATE_TIMESTAMP_FORMAT)) if delivery_date else None

                    cursor.execute(
                        "SELECT unity_price FROM operations.products WHERE product_id = %s",
                        (product_id,)
                    )
                    price_row = cursor.fetchone()
                    unity_price = price_row[0] if price_row else 0.00
                    total_amount = float(quantity) * float(unity_price)

                    cursor.execute("""
                                   INSERT INTO analytics.customer_order_items (order_id, order_item_id, customer, product,
                                                                               order_date, delivery_date, status, quantity,
                                                                               unity_price, total_amount, updated_at)
                                   VALUES (%s, %s,
                                           COALESCE((SELECT customer FROM analytics.dim_customers WHERE customer_id = %s AND is_current = TRUE), 1),
                                           COALESCE((SELECT product FROM analytics.dim_products WHERE product_id = %s AND is_current = TRUE), 1),
                                           %s, %s, %s, %s, %s, %s, NOW())
                                       ON CONFLICT (order_id, order_item_id) 
                                       DO UPDATE SET status = EXCLUDED.status,
                                                                                     quantity = EXCLUDED.quantity,
                                                                                     total_amount = EXCLUDED.total_amount,
                                                                                     delivery_date = EXCLUDED.delivery_date,
                                                                                     updated_at = NOW();
                                   """, (order_id, order_item_id, cust_id, product_id, order_date, delivery_date, status, quantity, unity_price, total_amount))

                # Commit transaction block on processing success
                conn.commit()

            except Exception as error:
                print(f"❌ Error encountered processing streaming packet record: {error}")
                conn.rollback()

    except KeyboardInterrupt:
        print("\nStopping streaming ingestion gracefully...")
    finally:
        consumer.close()

if __name__ == "__main__":
    process_stream()