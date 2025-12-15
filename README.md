# AIS Telemetry Streaming Pipeline

- System Design: Docker Compose (Ingestion, Broker, Processor, DB, UI)

- Stream Processing: Real-time pipeline with Quix Streams (Kafka) and H3 geospatial indexing to process, classify, and aggregate raw AIS maritime telemetry.

- Time-Series Database: TimescaleDB with continuous aggregates for efficient, real-time analytical queries.

- BI: Grafana dashboard with SQL window functions.

<img width="1688" height="849" alt="Screenshot 2025-12-14 064506" src="https://github.com/user-attachments/assets/f067d3ca-fc55-4173-b368-6782a4bf8375" />

#
