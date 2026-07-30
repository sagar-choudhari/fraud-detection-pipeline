# Real-Time Fraud Detection Pipeline

A production-grade data engineering portfolio project implementing a real-time fraud detection system using Apache Kafka, Apache Spark Structured Streaming, Delta Lake, and AWS S3 — built entirely in Java.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         LOCAL DEVELOPMENT                               │
│                                                                         │
│   ┌──────────────┐     ┌───────────┐     ┌────────────────────────┐     │
│   │ Transaction  │────▶│   Kafka   │────▶│  Spark Structured      │     │
│   │ Producer     │     │ (Docker)  │     │  Streaming             │     │
│   │ (Java)       │     │           │     │  (Java, local[*])      │     │
│   └──────────────┘     └───────────┘     └───────────┬────────────┘     │
│                                                      │                  │
└──────────────────────────────────────────────────────┼──────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          AWS S3 — MEDALLION ARCHITECTURE                │
│                                                                         │
│   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐       │
│   │     BRONZE      │   │     SILVER      │   │      GOLD       │       │
│   │  transactions/  │──▶│  transactions/  │──▶│  transactions/  │       │
│   │  raw/           │   │  curated/       │   │  fraud_alerts/  │       │
│   │                 │   │                 │   │                 │       │
│   │  Raw append     │   │  MERGE INTO     │   │  Fraud rules    │       │
│   │  No transforms  │   │  Deduplication  │   │  output         │       │
│   │  Delta Lake     │   │  Delta Lake     │   │  Delta Lake     │       │
│   └─────────────────┘   └─────────────────┘   └─────────────────┘       │
│                                                                         │
│   ┌─────────────────┐   ┌─────────────────┐                             │
│   │    ORPHANED     │   │   CHECKPOINTS   │                             │
│   │  transactions/  │   │  transactions/  │                             │
│   │  orphaned/      │   │  checkpoints/   │                             │
│   │                 │   │                 │                             │
│   │  Bad records    │   │  Spark offset   │                             │
│   │  Dead letter    │   │  tracking       │                             │
│   └─────────────────┘   └─────────────────┘                             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │     AWS Glue          │
                    │  PySpark Fraud Job    │
                    │  (Serverless Spark)   │
                    └───────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 7.4.0 (Confluent, Docker) |
| Stream Processing | Apache Spark Structured Streaming 3.5.1 |
| Storage Format | Delta Lake 3.2.0 |
| Cloud Storage | AWS S3 (s3a protocol via Hadoop AWS) |
| Serverless Compute | AWS Glue (PySpark) |
| Language | Java 17 (pipeline), Python (Glue) |
| Build Tool | Maven |
| Infrastructure | Docker Compose |

---

## Data Flow

### 1. Transaction Producer
Simulates real-time banking transactions at 500ms intervals. Each transaction contains:

```json
{
  "txnId": "677469ec-c4fd-4fc3-8cef-050afbe6a722",
  "userId": "user_82",
  "amount": 8760.48,
  "merchant": "ICICI_ATM",
  "timestamp": "2026-07-30T04:51:22"
}
```

In production, this feed comes from upstream banking systems via Spring Boot microservices publishing to Kafka topics.

### 2. Spark Structured Streaming Consumer
Reads from Kafka, parses JSON into structured DataFrame, and writes to S3 via `foreachBatch`:

- **Bronze** — raw records appended as-is. Source of truth for replay.
- **Silver** — clean records upserted via `DeltaTable.merge()`. Deduplication key: `txnId`.
- **Orphaned** — null/invalid records parked for investigation. Dead letter queue pattern.

### 3. Fraud Detection (Batch on Silver)
Reads curated Silver layer, applies three fraud rules, writes alerts to Gold.

---

## Fraud Detection Rules

### Rule 1 — High Value Transaction
Flags any transaction above ₹8,000 for manual review.

```
Signal: amount > 8000
Flag:   HIGH_VALUE
```

### Rule 2 — Card Testing Attack
Detects when the same user hits the same merchant more than twice — a common pattern where fraudsters test stolen cards with small amounts before making large purchases.

```
Signal: COUNT(txnId) OVER (PARTITION BY userId, merchant) > 2
Flag:   CARD_TESTING
```

### Rule 3 — High Velocity
Flags users making more than 3 transactions within a 1-hour tumbling window — indicative of account takeover or automated fraud.

```
Signal: COUNT(txnId) > 3 within 1-hour window
Flag:   HIGH_VELOCITY
```

---

## Sample Output

```
Total number of transactions: 169

High Value transactions
+--------------------------------------+---------+-------+----------+---------------------+------------+
|txn_id                                |user_id  |amount |merchant  |timestamp            |fraud_flag  |
+--------------------------------------+---------+-------+----------+---------------------+------------+
|eb7cda3d-d52f-4fb3-8cfa-bf8d85d79ee9 |user_86  |8366.39|Ola       |2026-07-30 04:51:22  |HIGH_VALUE  |
|e47870ae-c693-44aa-8774-e51e6cc6fc48 |user_63  |8414.61|Swiggy    |2026-07-30 04:51:23  |HIGH_VALUE  |
|0c734e4e-354a-4f7e-9cbb-ccc0a85b4fa8 |user_28  |9732.73|HDFC_ATM  |2026-07-30 04:51:24  |HIGH_VALUE  |
+--------------------------------------+---------+-------+----------+---------------------+------------+

Possible Card Testing Attack
+--------------------------------------+---------+-------+---------+----------+---------------------+-------------+
|txn_id                                |user_id  |amount |txn_count|merchant  |timestamp            |fraud_flag   |
+--------------------------------------+---------+-------+---------+----------+---------------------+-------------+
|639c885f-ed58-4085-bcb5-0a7f5ea31007 |user_18  |4666.08|3        |ICICI_ATM |2026-07-30 04:51:29  |CARD_TESTING |
|32f70615-27b1-420c-a727-c733508c9534 |user_20  |3202.96|3        |ICICI_ATM |2026-07-30 04:51:56  |CARD_TESTING |
|55c8e723-c16d-4af7-b1c4-f42d5d34d4c7 |user_8   |5552.12|3        |Zomato    |2026-07-30 04:51:31  |CARD_TESTING |
+--------------------------------------+---------+-------+---------+----------+---------------------+-------------+

High Velocity transactions
+---------+---------------------+---------------------+---------+--------------+-------------+
|user_id  |window_start         |window_end           |txn_count|total_amount  |fraud_flag   |
+---------+---------------------+---------------------+---------+--------------+-------------+
|user_71  |2026-07-30 04:30:00  |2026-07-30 05:30:00  |5        |13254.57      |HIGH_VELOCITY|
|user_97  |2026-07-30 04:30:00  |2026-07-30 05:30:00  |5        |16032.87      |HIGH_VELOCITY|
|user_40  |2026-07-30 04:30:00  |2026-07-30 05:30:00  |5        |27253.43      HIGH_VELOCITY|
+---------+---------------------+---------------------+---------+--------------+-------------+

Fraud alerts written to gold layer. Total alerts: 57
```

---

## Project Structure

```
fraud-detection-pipeline/
├── docker-compose.yml                        # Kafka + Zookeeper + Kafka UI
├── .mvn/
│   └── jvm.config                            # Java 17 module open flags for Spark
├── src/
│   └── main/
│       ├── java/com/frauddetection/
│       │   ├── config/
│       │   │   ├── ConfigLoader.java         # Reads application.properties
│       │   ├── model/
│       │   │   └── Transaction.java          # Transaction POJO + factory method
│       │   ├── producer/
│       │   │   └── TransactionProducer.java  # Kafka producer (data simulator)
│       │   ├── spark/
│       │   │   └── SparkSessionFactory.java  # Singleton SparkSession with Delta + S3 config
│       │   ├── streaming/
│       │   │   └── TransactionStreamReader.java  # Structured Streaming + medallion writes
│       │   ├── fraud/
│       │   │   └── FraudDetection.java       # Batch fraud rules on Silver layer
│       │   └── utils/
│       │       └── DeltaReader.java          # Dev utility — batch read from Delta
│       └── resources/
│           └── application.properties.template  # Config template (copy to .properties)
└── pom.xml
```

---

## How To Run Locally

### Prerequisites
- Java 17
- Maven 3.8+
- Docker Desktop (WSL2 backend)
- IntelliJ IDEA Community
- AWS CLI configured (`aws configure`)

### 1. Clone and Configure

```bash
git clone https://github.com/sagar-choudhari/fraud-detection-pipeline
cd fraud-detection-pipeline
cp src/main/resources/application.properties.template src/main/resources/application.properties
# Edit application.properties with your S3 bucket and Kafka settings
```

### 2. Start Kafka

```bash
docker compose up -d
docker ps  # verify zookeeper and kafka are running
```

### 3. Create Kafka Topic

The `transactions` topic is created automatically when the producer
first publishes to it. Confluent Kafka image has `auto.create.topics.enable=true`
by default.

Optionally verify topic exists after running producer:
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 4. Run the Pipeline

Open two IntelliJ run configurations simultaneously:

```
Terminal 1: TransactionProducer     — sends fake transactions to Kafka
Terminal 2: TransactionStreamReader — reads from Kafka, writes to S3 Delta Lake
```

### 5. Run Fraud Detection

```bash
# After transactions are flowing into Silver layer
# Run FraudDetection.java — no Kafka or Docker needed
# Reads from S3 Silver, writes alerts to S3 Gold
```

---

## AWS Setup

### S3 Bucket Structure

```
s3://your-bucket/
└── transactions/
    ├── raw/          # Bronze — Delta Lake
    ├── curated/      # Silver — Delta Lake (MERGE INTO)
    ├── orphaned/     # Silver — bad records
    ├── fraud_alerts/ # Gold  — Delta Lake
    └── checkpoints/  # Spark streaming checkpoints
```

### Required IAM Permissions

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::your-bucket",
    "arn:aws:s3:::your-bucket/*"
  ]
}
```

---

## Key Engineering Decisions

**Why Kafka?** Decouples producer from consumer. Producer (banking system) and consumer (Spark job) are completely independent — one can be down without affecting the other. Messages retained for 7 days.

**Why Delta Lake over Parquet?** ACID guarantees, MERGE INTO for deduplication, time travel for regulatory audits. Raw Parquet has no protection against partial writes or duplicate records.

**Why MERGE INTO (upsert) over append?** Banking systems retry failed requests. The same transaction can arrive twice from Kafka. Append would create duplicates corrupting fraud analysis. MERGE INTO on `txnId` (UUID) ensures exactly-once semantics at the storage layer.

**Why Medallion Architecture?** Each layer serves a different consumer. Bronze preserves raw data for replay. Silver is the single source of truth for analytics. Gold is optimized for dashboards and alerting systems. Schema changes in one layer don't break others.

**Why `foreachBatch`?** `MERGE INTO` is a batch operation — it needs a complete, finite DataFrame to compare against the existing Delta table. `foreachBatch` intercepts each Spark micro-batch and exposes it as a regular batch DataFrame, enabling upsert logic inside a streaming job.

---

## Target Role

Built as a portfolio project targeting **Data Engineer** roles at BFSI and GCC companies (Barclays, Deutsche Bank).

Demonstrates: Kafka producer/consumer, Spark Structured Streaming internals, Delta Lake ACID operations, AWS S3 integration, medallion lakehouse architecture, and fraud detection SQL logic.

---

*Built with Java 17 · Apache Spark 3.5.1 · Delta Lake 3.2.0 · Apache Kafka 3.x · AWS S3*