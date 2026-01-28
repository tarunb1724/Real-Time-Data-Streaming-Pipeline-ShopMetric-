# Real-Time-Data-Streaming-Pipeline-ShopMetric-
ShopMetric is a scalable, real-time ETL pipeline that ingests simulated high-volume e-commerce orders via Kafka, processes them with Spark Structured Streaming, and archives raw data to AWS S3 while serving analytics through Snowflake.

# 🛒 ShopMetric — Real-Time E-Commerce Data Pipeline

**ShopMetric** is an end-to-end **real-time data engineering project** that simulates, processes, stores, and visualizes e-commerce transactions at scale.

It demonstrates how modern streaming systems work together using **Apache Kafka**, **Apache Spark Structured Streaming**, **AWS S3**, and **Snowflake**, following real-world data architecture best practices.

---

## 📌 Project Highlights

- Real-time event streaming with **Kafka**
- Distributed stream processing with **Spark**
- Hybrid storage architecture:
  - **AWS S3** → Data Lake (raw & historical data)
  - **Snowflake** → Data Warehouse (analytics-ready data)
- Fully containerized with **Docker & Docker Compose**
- Designed for **scalability, fault tolerance, and analytics**

---

## 🏗 Architecture — *The Pizza Shop Model 🍕*

| Layer | Component | Description |
|-----|----------|-------------|
| 🧑‍🤝‍🧑 Source | Customers | Python script generates fake orders |
| 🧾 Ingestion | Kafka (Waiter) | Buffers high-throughput events |
| 👨‍🍳 Processing | Spark (Chef) | Cleans & filters streaming data |
| 🗄 Storage | AWS S3 (Basement) | Stores raw historical data |
| 📊 Analytics | Snowflake (Display) | Stores curated analytics data |
| 📈 Visualization | Streamlit (Scoreboard) | Live dashboards *(in progress)* |

---

## 🛠 Tech Stack

- **Language:** Python 3.9+
- **Containerization:** Docker, Docker Compose
- **Message Broker:** Apache Kafka, Zookeeper
- **Stream Processing:** Apache Spark (PySpark)
- **Data Lake:** AWS S3
- **Data Warehouse:** Snowflake *(In Progress)*
- **Visualization:** Streamlit *(In Progress)*

---

## 📂 Project Structure

```text
ShopMetric/
│
├── docker-compose.yml
├── scripts/
│   └── generator.py           # Kafka data generator
│
├── spark_processor.py         # Spark streaming job
│
├── user-jars/                 # Spark external JARs
│
├── README.md
└── requirements.txt

### 2. Clone the Repository
```bash
git clone [https://github.com/yourusername/ShopMetric.git](https://github.com/yourusername/ShopMetric.git)
cd ShopMetric
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install kafka-python faker pyspark
docker-compose up -d
python scripts/generator.py
