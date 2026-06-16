## Data Engineering Take-Home Task

### Welcome

Welcome to Deel's Data Engineering Take-Home task, as mentioned in the Task specification document, this is the pre-built stack that will help you on your solution development. This repository contains a pre-configured database containing the database represented by the following DER:


![Database Diagram](./diagrams/database-diagram.png)


### Database Configuration

Once you have [Docker](https://www.docker.com/products/docker-desktop/) and [docker-compose](https://docs.docker.com/compose/install/) configured in your computer, with your Docker engine running, you must execute the following command provision the source database:


> docker-compose up


:warning:**Important**: Before running this command make sure you're in the root folder of the project.

Once you have the Database up and running feel free to connect to this using any tool you want, for this you can use the following credentials:

- **Username**: `finance_db_user`
- **Password**: `1234`
- **Database**: `finance_db`

### Debezium CDC

The stack includes a Debezium CDC pipeline that streams database changes to Kafka in real-time. Kafka is available at `localhost:9092`.

#### Topics

| Kafka Topic | Source Table |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

#### Kafka Connection Example

```properties
bootstrap.servers=localhost:9092
```

Extra informations and tips about the task execution can be found in the task description document shared by our recruiting team.

For any questions, feel free to reach us out through data-platform@deel.com

---
---

# Solution — Acme Financial Analytics

A real-time CDC pipeline that captures transactional changes from PostgreSQL, streams them through Kafka via Debezium, and materialises a star-schema analytics layer — all exposed through a live FastAPI.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PostgreSQL (finance_db)                       │
│                                                                      │
│  operations schema (source)      analytics schema (warehouse)        │
│  ┌──────────┐ ┌──────────┐      ┌──────────────┐ ┌─────────────┐  │
│  │customers │ │ products │      │dim_customers │ │dim_products │  │
│  └──────────┘ └──────────┘      └──────────────┘ └─────────────┘  │
│  ┌──────────┐ ┌────────────┐    ┌──────────────┐ ┌─────────────┐  │
│  │  orders  │ │order_items │    │  dim_dates   │ │staging_*    │  │
│  └──────────┘ └────────────┘    └──────────────┘ └─────────────┘  │
│                                 ┌──────────────────────────────┐   │
│  pg_cron seeds data every 1-2m  │  customer_order_items (fact)  │  │
└─────────────────────┬───────────┴──────────────────────────────┴───┘
                      │ WAL / logical replication
                      ▼
           ┌──────────────────────┐
           │  Debezium / Kafka     │
           │  Connect  (:8083)     │
           └──────────┬───────────┘
                      │ CDC messages (4 topics)
                      ▼
           ┌──────────────────────┐
           │         Kafka         │
           │       (:9092)         │
           └──────────┬───────────┘
                      │ consumer
                      ▼
           ┌──────────────────────┐       ┌──────────────────────┐
           │  streaming-pipeline   │──────►│  analytics schema     │
           │  (stream_worker.py)   │ write │  (PostgreSQL)         │
           └──────────────────────┘       └──────────┬───────────┘
                                                      │ read
                                                      ▼
                                          ┌──────────────────────┐
                                          │    analytics-api       │
                                          │    FastAPI  (:8000)    │
                                          └──────────────────────┘
```

---

## Analytics Schema (Star Schema)

```
                    ┌─────────────────────────┐
                    │        dim_dates          │
                    │─────────────────────────  │
                    │ PK dim_date  (YYYYMMDD)   │
                    │    calendar_date           │
                    │    day_of_week             │
                    │    month / quarter / year  │
                    └────────────┬──────────────┘
                                 │ FK (order_date, delivery_date)
                                 │
┌─────────────────────┐          │          ┌─────────────────────┐
│    dim_customers     │          │          │    dim_products      │
│─────────────────────│          │          │─────────────────────│
│ PK customer (serial)│          │          │ PK product  (serial)│
│    customer_id       │          │          │    product_id        │
│    customer_name     │          │          │    product_name      │
│    customer_address  │          │          │    barcode           │
│    valid_from        │          │          │    valid_from        │
│    valid_to          │          │          │    valid_to          │
│    is_current (SCD2) │          │          │    is_current (SCD2)│
└──────────┬──────────┘          │          └──────────┬──────────┘
           │ FK customer          │           FK product │
           │                      ▼                      │
           │        ┌─────────────────────────┐          │
           └───────►│   customer_order_items   │◄─────────┘
                    │        (fact table)       │
                    │─────────────────────────  │
                    │ PK customer_order_item    │
                    │    order_id               │
                    │    order_item_id          │
                    │    customer  (FK)         │
                    │    product   (FK)         │
                    │    order_date    (FK)     │
                    │    delivery_date (FK)     │
                    │    status                 │
                    │    quantity               │
                    │    unity_price            │
                    │    total_amount           │
                    │    updated_at             │
                    └───────────────────────────┘

Staging tables (buffer for out-of-order CDC events):

  staging_orders       — holds order events until their order_items arrive
  staging_order_items  — holds order_item events until their order arrives
```

**SCD Type 2** is applied to both dimension tables. When a customer or product changes, the existing row is closed (`is_current = FALSE`, `valid_to = NOW()`) and a new row is inserted with the latest values. The fact table stores the surrogate key (`customer` / `product` serial), preserving the historical snapshot at the time of the order.

---

## Pipeline — `pipeline/stream_worker.py`

The streaming pipeline is a long-running Python process that consumes CDC events from Kafka and maintains the analytics star schema in real time.

**Kafka topics consumed:**

| Topic | Source |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

**How it works:**

1. **Startup retries** — both the Kafka and PostgreSQL connections have automatic retry loops; the pipeline waits until both are ready before consuming.
2. **Poll loop** — polls Kafka with a 1-second timeout, decodes each message as a Debezium JSON envelope, and extracts the `after` payload.
3. **Event routing** — dispatches each message to a handler based on the topic:
   - `handle_customer` / `handle_product` — SCD Type 2 upsert: closes the current dimension row and inserts a new one.
   - `handle_order` — upserts into `staging_orders`, propagates status changes to existing fact rows, and calls `merge_into_fact` for any order_items that already arrived.
   - `handle_order_item` — upserts into `staging_order_items`, then immediately calls `merge_into_fact`.
4. **`merge_into_fact`** — joins both staging tables. If both sides exist it resolves dimension surrogates, fetches the current unit price, computes `total_amount`, and upserts into `customer_order_items`. If one side is missing it returns silently and is triggered again when the missing event arrives.
5. **Atomicity** — each message is committed atomically; errors are logged and rolled back without crashing the process.

**Build and start:**

```bash
docker compose up -d --build streaming-pipeline
```

**Tail logs:**

```bash
docker compose logs -f streaming-pipeline
```

---

## API — `api/main.py`

The analytics API is a FastAPI application that queries `analytics.customer_order_items` directly, using a `SimpleConnectionPool` (1–20 connections) for concurrency.

**Build and start:**

```bash
docker compose up -d --build analytics-api
```

**Interactive docs (Swagger UI):**

```
http://localhost:8000/docs#/
```

The Swagger UI lists every endpoint with live try-it-out support — fill in the parameters and execute queries directly from the browser.

### Endpoints

All endpoints accept the following optional query parameters:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `status` | string | `open` | Filter by order status (case-insensitive) |
| `date_from` | date | — | Filter delivery dates from this date (inclusive) |
| `date_to` | date | — | Filter delivery dates up to this date (inclusive) |

---

#### `GET /analytics/orders`

Aggregate order counts grouped by `delivery_date` and `status`.

```
GET /analytics/orders?status=open&date_from=2024-01-01&date_to=2024-03-31
```

```json
[{ "delivery_date": "2024-03-15", "status": "open", "order_count": 42 }]
```

---

#### `GET /analytics/orders/top`

Top N delivery dates by order volume. Extra param: `limit` (default 3, max 100).

```
GET /analytics/orders/top?status=open&limit=3
```

```json
[
  { "delivery_date": "2024-03-15", "open_order_count": 120 },
  { "delivery_date": "2024-03-16", "open_order_count": 98 }
]
```

---

#### `GET /analytics/orders/product`

Total pending quantity per product, ordered highest first.

```
GET /analytics/orders/product?status=open
```

```json
[{ "product_id": 7, "total_pending_quantity": 4500 }]
```

---

#### `GET /analytics/orders/customers/`

Top N customers by number of open orders. Extra param: `limit` (default 3, max 100).

```
GET /analytics/orders/customers/?status=open&limit=3
```

```json
[{ "customer_id": 3, "pending_order_count": 18 }]
```

---

## Running the Full Stack

Start everything (first run or after any changes):

```bash
docker compose up -d --build
```

Rebuild only the streaming pipeline:

```bash
docker compose up -d --build streaming-pipeline
```

Rebuild only the API:

```bash
docker compose up -d --build analytics-api
```

Stop everything:

```bash
docker compose down
```

Stop and wipe the database volume (full reset):

```bash
docker compose down -v
```
