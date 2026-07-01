# SentinelChain — Implementation Plan

> **Real-Time Supply Chain Risk Intelligence & Hybrid RAG Copilot**  
> Mục tiêu: xây dựng một hệ thống production-like thể hiện đồng thời năng lực **AI/ML, Data Engineering, Streaming, RAG, MLOps, Backend và DevOps**.

---

## 1. Tóm tắt dự án

SentinelChain là nền tảng theo dõi rủi ro chuỗi cung ứng theo thời gian thực. Hệ thống liên tục thu thập:

- Tin tức và sự kiện toàn cầu.
- Thiên tai và sự kiện địa lý.
- Hồ sơ doanh nghiệp.
- Dữ liệu nội bộ về supplier, facility, shipment, purchase order và inventory.

Các nguồn dữ liệu được đẩy vào Kafka, chuẩn hóa và xử lý bằng Apache Flink. Hệ thống sử dụng NLP để trích xuất thực thể và loại sự kiện, liên kết sự kiện với supplier/facility nội bộ, tính toán mức độ ảnh hưởng theo không gian và thời gian, sau đó chấm điểm rủi ro bằng mô hình ML.

Bên cạnh pipeline cảnh báo, hệ thống cung cấp một **Hybrid RAG Copilot** kết hợp:

- Vector search trên tin tức, filing và báo cáo.
- SQL query trên shipment, inventory và purchase order.
- Geospatial query trên facility, warehouse và route.
- Risk-model output và feature explanation.
- Graph traversal ở giai đoạn mở rộng.

Ví dụ câu hỏi:

```text
Những shipment nào có nguy cơ bị chậm do trận động đất mới nhất tại Nhật Bản?
```

Câu trả lời phải bao gồm:

- Shipment bị ảnh hưởng.
- Supplier/facility liên quan.
- Khoảng cách tới tâm sự kiện.
- Inventory days remaining.
- Risk score.
- Nguồn bằng chứng.
- Timestamp và model version.

---

## 2. Mục tiêu

### 2.1. Mục tiêu kỹ thuật

1. **Real-time ingestion**
   - Polling hoặc stream từ nguồn công khai đang cập nhật thật.
   - CDC từ PostgreSQL bằng Debezium.

2. **Streaming Data Engineering**
   - Kafka topic design.
   - Schema Registry.
   - Event-time processing.
   - Watermark.
   - Deduplication.
   - Stateful join.
   - Window aggregation.
   - Retry và dead-letter queue.

3. **AI/NLP**
   - Event classification.
   - Named Entity Recognition.
   - Entity linking.
   - Document chunking.
   - Embedding.
   - Hybrid retrieval.
   - LLM generation.
   - Grounding và output validation.

4. **Machine Learning**
   - Feature engineering.
   - Risk classification/ranking.
   - Online inference.
   - Explainability.
   - MLflow model registry.
   - Drift monitoring.

5. **MLOps và DevOps**
   - Docker Compose.
   - Kubernetes.
   - Terraform/Helm.
   - CI/CD.
   - Prometheus, Grafana và OpenTelemetry.

### 2.2. Mục tiêu nghiệp vụ

Hệ thống phải trả lời được:

- Sự kiện nào đang có khả năng ảnh hưởng đến chuỗi cung ứng?
- Supplier/facility nào nằm trong vùng bị ảnh hưởng?
- Shipment và purchase order nào có nguy cơ trễ?
- Sản phẩm nào phụ thuộc vào supplier đó?
- Inventory hiện tại đủ dùng bao nhiêu ngày?
- Vì sao risk score tăng?
- Nguồn bằng chứng nào hỗ trợ cảnh báo?
- Có supplier thay thế không?

---

## 3. Phạm vi

### 3.1. MVP bắt buộc

- USGS Earthquake ingestion.
- GDELT hoặc GDACS ingestion.
- PostgreSQL operational simulator.
- Debezium CDC.
- Kafka + Schema Registry.
- Flink normalization, deduplication và stateful joins.
- Geospatial impact correlation.
- Rule-based risk scoring.
- Alert API và dashboard.
- Document chunking và embedding.
- OpenSearch hybrid search.
- RAG Copilot.
- Citation validation.
- Prometheus/Grafana.
- Docker Compose chạy end-to-end.

### 3.2. Phần nâng cao

- NASA FIRMS.
- SEC EDGAR.
- ML-based risk scoring.
- Supplier graph.
- Alternative supplier recommendation.
- Flink online model inference.
- Apache Iceberg lakehouse.
- Kubernetes deployment.
- Multi-tenant access control.
- Human feedback và active learning.

### 3.3. Không thuộc phạm vi ban đầu

- Dữ liệu logistics thương mại trả phí.
- Tích hợp ERP thật.
- Tự động đưa ra quyết định nghiệp vụ không có human review.
- Cam kết chính xác tuyệt đối về tác động của thiên tai.

---

## 4. Personas

### Supply Chain Analyst

- Theo dõi cảnh báo.
- Kiểm tra shipment bị ảnh hưởng.
- Hỏi Copilot để điều tra nguyên nhân.

### Risk Manager

- Theo dõi critical alerts.
- Kiểm tra nguồn bằng chứng.
- Phê duyệt hoặc bác bỏ cảnh báo.

### Data Engineer

- Theo dõi consumer lag.
- Kiểm tra data quality.
- Xử lý DLQ.
- Quản lý schema evolution.

### ML Engineer

- Huấn luyện risk model.
- Theo dõi model drift.
- So sánh model version.
- Phân tích false positive/false negative.

---

## 5. Kiến trúc tổng thể

```mermaid
flowchart LR
    subgraph External["External Live Sources"]
        USGS["USGS Earthquake"]
        GDACS["GDACS"]
        GDELT["GDELT"]
        FIRMS["NASA FIRMS"]
        SEC["SEC EDGAR"]
    end

    subgraph Operational["Operational Domain"]
        SIM["Supply Chain Simulator"]
        PG["PostgreSQL"]
        DBZ["Debezium CDC"]
        SIM --> PG --> DBZ
    end

    subgraph Streaming["Streaming Platform"]
        KAFKA["Apache Kafka"]
        SR["Schema Registry"]
        CONNECT["Kafka Connect"]
        FLINK["Apache Flink"]
    end

    subgraph AI["AI Processing"]
        NLP["NLP Enrichment"]
        ER["Entity Resolution"]
        GEO["Geospatial Correlation"]
        RISK["Risk Scoring"]
        CHUNK["Chunking Service"]
        EMB["Embedding Service"]
        LLM["LLM Gateway"]
    end

    subgraph Storage["Serving & Analytical Stores"]
        OS["OpenSearch"]
        REDIS["Redis"]
        OLTP["PostgreSQL Read Models"]
        ICE["Apache Iceberg / MinIO"]
        MLFLOW["MLflow"]
    end

    subgraph Apps["Applications"]
        API["FastAPI"]
        UI["React Dashboard"]
        RAG["Hybrid RAG Orchestrator"]
    end

    USGS --> KAFKA
    GDACS --> KAFKA
    GDELT --> KAFKA
    FIRMS --> KAFKA
    SEC --> KAFKA
    DBZ --> KAFKA

    KAFKA <--> FLINK
    SR --- KAFKA
    CONNECT --- KAFKA

    FLINK --> NLP --> ER --> GEO --> RISK --> KAFKA
    KAFKA --> CHUNK --> EMB --> OS
    FLINK --> OS
    FLINK --> ICE
    RISK --> MLFLOW

    KAFKA --> API --> UI
    API --> RAG
    RAG --> OS
    RAG --> OLTP
    RAG --> REDIS
    RAG --> LLM
```

---

## 6. Công nghệ

| Nhóm | Công nghệ |
|---|---|
| Streaming | Apache Kafka, Kafka Connect, Schema Registry |
| Stream Processing | Apache Flink SQL, Flink DataStream API |
| CDC | Debezium PostgreSQL Connector |
| Operational DB | PostgreSQL |
| Search | OpenSearch: BM25, vector search, geospatial |
| Cache | Redis |
| Lakehouse | Apache Iceberg + MinIO |
| Backend | FastAPI |
| Frontend | React, TypeScript, MapLibre hoặc Deck.gl |
| AI/NLP | Hugging Face, sentence-transformers, spaCy hoặc GLiNER |
| LLM | OpenAI-compatible gateway hoặc local vLLM |
| ML | LightGBM/XGBoost, scikit-learn |
| MLOps | MLflow |
| Observability | Prometheus, Grafana, OpenTelemetry |
| Local infra | Docker Compose |
| Production | Kubernetes, Helm, Terraform |
| CI/CD | GitHub Actions |

---

## 7. Nguồn dữ liệu

### 7.1. USGS Earthquake

Dùng feed GeoJSON cập nhật thường xuyên.

```json
{
  "source_event_id": "us7000xxxx",
  "magnitude": 6.2,
  "latitude": 35.1,
  "longitude": 139.2,
  "depth_km": 38.0,
  "place": "Near the east coast of Honshu",
  "event_time": "2026-07-01T09:10:00Z",
  "updated_time": "2026-07-01T09:18:00Z",
  "source_url": "..."
}
```

Ingestion service phải:

- Poll theo chu kỳ cấu hình.
- Lưu cursor/last-seen timestamp.
- Không phát lại event không thay đổi.
- Phát event mới khi upstream cập nhật magnitude hoặc metadata.
- Gắn `source_version`.

### 7.2. GDACS

Dùng cho earthquake, flood, cyclone, volcano và wildfire. Giữ alert level, severity, country, geometry, population exposure, event lifecycle.

### 7.3. GDELT

Dùng để lấy article metadata, organizations, locations, themes, tone, URL và publish timestamp.

Triển khai theo hai mức:

1. MVP: poll API theo query.
2. Nâng cao: tải và xử lý các update file định kỳ.

### 7.4. NASA FIRMS

Dùng ở giai đoạn nâng cao cho active fire/hotspot.

### 7.5. SEC EDGAR

Dùng ở giai đoạn nâng cao cho 8-K, 10-Q và 10-K.

### 7.6. Operational Supply Chain Simulator

Simulator phát sinh dữ liệu liên tục vào PostgreSQL:

- Supplier.
- Facility.
- Product.
- Purchase order.
- Shipment.
- Shipment event.
- Inventory.
- Warehouse.
- Route.

Hành vi mô phỏng:

- Tạo PO mới.
- Shipment chuyển trạng thái.
- ETA thay đổi.
- Inventory tăng/giảm.
- Supplier capacity thay đổi.
- Delay ngẫu nhiên.
- Facility tạm ngừng.
- Route thay đổi.

Debezium chuyển thay đổi thành Kafka CDC events.

---

## 8. Mô hình dữ liệu nghiệp vụ

### 8.1. Supplier

```sql
CREATE TABLE suppliers (
    supplier_id UUID PRIMARY KEY,
    supplier_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    country_code CHAR(2),
    supplier_tier INTEGER NOT NULL,
    criticality_score NUMERIC(5,4) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

### 8.2. Facility

```sql
CREATE TABLE facilities (
    facility_id UUID PRIMARY KEY,
    supplier_id UUID NOT NULL REFERENCES suppliers(supplier_id),
    facility_name TEXT NOT NULL,
    facility_type TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    h3_index TEXT,
    country_code CHAR(2),
    capacity_score NUMERIC(5,4),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

### 8.3. Shipment

```sql
CREATE TABLE shipments (
    shipment_id UUID PRIMARY KEY,
    purchase_order_id UUID NOT NULL,
    supplier_id UUID NOT NULL,
    origin_facility_id UUID NOT NULL,
    destination_warehouse_id UUID NOT NULL,
    status TEXT NOT NULL,
    transport_mode TEXT NOT NULL,
    planned_departure_at TIMESTAMPTZ,
    planned_arrival_at TIMESTAMPTZ,
    estimated_arrival_at TIMESTAMPTZ,
    actual_arrival_at TIMESTAMPTZ,
    shipment_value NUMERIC(18,2),
    updated_at TIMESTAMPTZ NOT NULL
);
```

### 8.4. Inventory

```sql
CREATE TABLE inventory (
    warehouse_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity_on_hand NUMERIC(18,4) NOT NULL,
    average_daily_usage NUMERIC(18,4) NOT NULL,
    safety_stock NUMERIC(18,4) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (warehouse_id, product_id)
);
```

### 8.5. Purchase Order

```sql
CREATE TABLE purchase_orders (
    purchase_order_id UUID PRIMARY KEY,
    supplier_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity NUMERIC(18,4) NOT NULL,
    unit_price NUMERIC(18,4) NOT NULL,
    status TEXT NOT NULL,
    required_by_date DATE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

---

## 9. Event envelope chung

```json
{
  "event_id": "uuid",
  "event_type": "string",
  "event_version": 1,
  "source": "usgs",
  "source_event_id": "us7000xxxx",
  "source_version": "2026-07-01T09:18:00Z",
  "event_time": "2026-07-01T09:10:00Z",
  "ingested_at": "2026-07-01T09:18:05Z",
  "trace_id": "uuid",
  "correlation_id": "uuid-or-null",
  "tenant_id": "demo",
  "payload": {}
}
```

Quy tắc:

- `event_id`: duy nhất cho từng message.
- `source_event_id`: ổn định qua các lần cập nhật.
- `source_version`: dùng để deduplicate/update.
- `event_time`: thời gian nghiệp vụ.
- `ingested_at`: thời gian hệ thống nhận.
- `trace_id`: distributed tracing.
- `correlation_id`: workflow correlation.
- `event_version`: schema evolution.

---
## 10. Kafka topic design

### 10.1. External raw topics

| Topic | Key | Retention | Mục đích |
|---|---|---:|---|
| `ext.usgs.raw.v1` | `source_event_id` | 30 ngày | Raw earthquake events |
| `ext.gdacs.raw.v1` | `source_event_id` | 30 ngày | Raw disaster events |
| `ext.gdelt.raw.v1` | `article_id` | 14 ngày | Raw news metadata/content |
| `ext.firms.raw.v1` | `hotspot_id` | 14 ngày | Active fire events |
| `ext.sec.raw.v1` | `filing_id` | 90 ngày | Filing metadata/content |

### 10.2. Operational CDC topics

| Topic | Key |
|---|---|
| `ops.public.suppliers` | `supplier_id` |
| `ops.public.facilities` | `facility_id` |
| `ops.public.shipments` | `shipment_id` |
| `ops.public.inventory` | `warehouse_id + product_id` |
| `ops.public.purchase_orders` | `purchase_order_id` |

Các topic CDC giữ envelope Debezium gốc ở bronze layer. Flink tạo topic cleaned riêng.

### 10.3. Canonical topics

| Topic | Key | Cleanup |
|---|---|---|
| `events.normalized.v1` | `source_event_id` | delete |
| `events.deduplicated.v1` | `source_event_id` | delete |
| `events.classified.v1` | `source_event_id` | delete |
| `events.entities.v1` | `source_event_id` | delete |
| `events.geo_enriched.v1` | `source_event_id` | delete |
| `events.current.v1` | `source_event_id` | compact |

### 10.4. Risk topics

| Topic | Key |
|---|---|
| `risk.impact_candidates.v1` | `event_id + asset_id` |
| `risk.features.v1` | `impact_id` |
| `risk.scores.v1` | `impact_id` |
| `risk.alerts.v1` | `alert_id` |
| `risk.alerts.current.v1` | `alert_id` |
| `risk.alerts.retry.v1` | `alert_id` |
| `risk.alerts.dlq.v1` | `alert_id` |

### 10.5. RAG topics

| Topic | Key |
|---|---|
| `rag.documents.raw.v1` | `document_id` |
| `rag.documents.chunks.v1` | `chunk_id` |
| `rag.documents.embeddings.v1` | `chunk_id` |
| `rag.queries.v1` | `request_id` |
| `rag.retrieval_results.v1` | `request_id` |
| `rag.tool_calls.v1` | `request_id` |
| `rag.llm_requests.v1` | `request_id` |
| `rag.responses.raw.v1` | `request_id` |
| `rag.responses.validated.v1` | `request_id` |
| `rag.responses.retry.v1` | `request_id` |
| `rag.responses.dlq.v1` | `request_id` |

### 10.6. Audit topics

- `audit.data_quality.v1`
- `audit.model_inference.v1`
- `audit.security.v1`
- `audit.pipeline_metrics.v1`

---

## 11. End-to-end data flow

### 11.1. Flow A — External event ingestion

```mermaid
sequenceDiagram
    participant SRC as External Source
    participant ING as Ingestion Service
    participant K as Kafka Raw Topic
    participant F as Flink Normalize
    participant C as Canonical Topic

    loop Poll interval
        ING->>SRC: Fetch changes since cursor
        SRC-->>ING: New/updated events
        ING->>ING: Validate + create envelope
        ING->>K: Produce keyed event
    end

    K->>F: Consume
    F->>F: Normalize schema
    F->>F: Validate event time
    F->>C: Produce normalized event
```

Yêu cầu:

- Retry theo exponential backoff.
- Circuit breaker.
- Cursor persistence.
- Idempotent producer.
- Không commit cursor trước khi Kafka acknowledge.
- Metrics: poll latency, records fetched, records produced, duplicate count.

### 11.2. Flow B — Deduplication và event versioning

Dedup key:

```text
source + source_event_id
```

Flink keyed state:

```json
{
  "latest_source_version": "...",
  "payload_hash": "...",
  "last_seen_at": "..."
}
```

Quy tắc:

- Event mới: emit.
- Payload không đổi: drop.
- Version mới hoặc payload đổi: emit update.
- Event bị upstream hủy: emit status update/tombstone.
- State TTL cấu hình theo source.

### 11.3. Flow C — NLP event enrichment

```text
events.deduplicated
    → text preparation
    → event classifier
    → NER
    → entity linker
    → events.entities
```

Output:

```json
{
  "source_event_id": "gdelt-123",
  "event_category": "factory_fire",
  "category_confidence": 0.91,
  "organizations": [
    {
      "mention": "ABC Semiconductors",
      "canonical_supplier_id": "uuid",
      "link_confidence": 0.88
    }
  ],
  "locations": [
    {
      "mention": "Osaka",
      "latitude": 34.6937,
      "longitude": 135.5023
    }
  ]
}
```

### 11.4. Flow D — Geospatial impact correlation

Input:

- Event có point/polygon.
- Facility current-state table.
- Warehouse current-state table.
- Route geometry ở giai đoạn nâng cao.

MVP dùng Haversine distance:

```text
distance_km = haversine(event_point, facility_point)
```

Candidate rule:

```text
distance_km <= threshold(event_type, severity)
```

| Event type | Severity condition | Radius |
|---|---|---:|
| Earthquake | magnitude >= 7 | 250 km |
| Earthquake | magnitude >= 6 | 120 km |
| Earthquake | magnitude >= 5 | 50 km |
| Wildfire | high confidence | 40 km |
| Flood | alert red | polygon intersection |

Output:

```json
{
  "impact_id": "uuid",
  "event_id": "uuid",
  "asset_type": "facility",
  "asset_id": "uuid",
  "distance_km": 34.7,
  "inside_affected_area": true,
  "geospatial_score": 0.88,
  "event_time": "...",
  "calculated_at": "..."
}
```

### 11.5. Flow E — Join với operational state

Flink giữ hoặc lookup:

- Supplier current state.
- Facility current state.
- Open purchase orders.
- Active shipments.
- Current inventory.

Join chain:

```text
Impact candidate
    → facility
    → supplier
    → open POs
    → active shipments
    → destination inventory
    → product criticality
```

Output `risk.features.v1`:

```json
{
  "impact_id": "uuid",
  "event_id": "uuid",
  "supplier_id": "uuid",
  "facility_id": "uuid",
  "shipment_ids": ["uuid"],
  "purchase_order_ids": ["uuid"],
  "event_severity": 0.84,
  "distance_score": 0.91,
  "supplier_criticality": 0.95,
  "inventory_days_remaining": 3.1,
  "open_order_value": 1200000,
  "active_shipment_count": 5,
  "alternative_supplier_count": 1,
  "source_confirmation_count": 3,
  "feature_version": "v1"
}
```

### 11.6. Flow F — Risk scoring

MVP rule-based score:

```text
risk_score =
    0.20 × event_severity
  + 0.15 × distance_score
  + 0.15 × supplier_criticality
  + 0.15 × inventory_shortage_score
  + 0.10 × shipment_exposure_score
  + 0.10 × source_confirmation_score
  + 0.10 × source_reliability
  + 0.05 × recency_score
```

Threshold:

```text
0.00–0.39 → low
0.40–0.59 → medium
0.60–0.79 → high
0.80–1.00 → critical
```

Output:

```json
{
  "impact_id": "uuid",
  "risk_score": 0.91,
  "risk_level": "critical",
  "model_name": "rule_based_risk",
  "model_version": "1.0.0",
  "top_factors": [
    {"feature": "inventory_days_remaining", "contribution": 0.19},
    {"feature": "supplier_criticality", "contribution": 0.15}
  ]
}
```

ML phase:

- Baseline: Logistic Regression.
- Main model: LightGBM hoặc XGBoost.
- Optional ranking: LambdaMART.
- Targets:
  - `disruption_within_7_days`.
  - `shipment_delay_gt_2_days`.
  - `critical_business_impact`.

### 11.7. Flow G — Alert generation

Alert tạo khi:

- Risk score vượt threshold.
- Alert tương tự chưa active.
- Sự kiện chưa hết hạn.
- Supplier/facility đang active.

Alert dedup key:

```text
event_id + facility_id + alert_type
```

Lifecycle:

```text
OPEN → ACKNOWLEDGED → INVESTIGATING → RESOLVED
                         ↓
                    FALSE_POSITIVE
```

---

## 12. RAG data augmentation pipeline

### 12.1. Nguyên tắc

Chunking không đặt trong Flink SQL thuần.

```text
raw document
    → document parser
    → chunking service
    → chunk topic
    → embedding service hoặc Flink ML_PREDICT
    → OpenSearch
```

Chunking cần HTML/PDF parser, tokenizer, semantic boundaries, metadata propagation và reprocessing độc lập. Flink tập trung vào streaming orchestration, state, join, window và model inference ở bước phù hợp.

### 12.2. Document schema

```json
{
  "document_id": "gdelt-article-123",
  "document_type": "news_article",
  "source": "gdelt",
  "title": "...",
  "content": "...",
  "language": "en",
  "published_at": "...",
  "updated_at": "...",
  "source_url": "...",
  "entities": [],
  "access_scope": "public",
  "version": 1
}
```

### 12.3. Chunk schema

```json
{
  "chunk_id": "gdelt-article-123::v1::chunk-0004",
  "document_id": "gdelt-article-123",
  "document_version": 1,
  "chunk_index": 4,
  "chunk_text": "...",
  "token_count": 420,
  "heading_path": ["Operations", "Supply disruption"],
  "source": "gdelt",
  "published_at": "...",
  "entity_ids": ["supplier-123"],
  "event_ids": ["event-456"],
  "content_hash": "sha256",
  "is_active": true
}
```

### 12.4. Chunking strategy

MVP:

- Recursive text splitter.
- Target: 350–500 tokens.
- Overlap: 50–80 tokens.
- Không tách giữa câu.
- Prefix title và heading vào chunk text.

Advanced:

- Semantic chunking.
- Layout-aware PDF chunking.
- Table extraction.
- Parent-child retrieval.
- Contextual chunk headers.

### 12.5. Embedding

Yêu cầu:

- Batch embedding.
- Rate limit.
- Retry.
- Idempotency theo `chunk_id + embedding_model_version`.
- Lưu dimension và model version.

```json
{
  "chunk_id": "...",
  "embedding": [0.1, 0.2],
  "embedding_model": "bge-m3",
  "embedding_dimension": 1024,
  "embedded_at": "...",
  "document_version": 1
}
```

### 12.6. OpenSearch index

Fields:

```json
{
  "chunk_id": "keyword",
  "document_id": "keyword",
  "chunk_text": "text",
  "title": "text",
  "source": "keyword",
  "published_at": "date",
  "entity_ids": "keyword",
  "event_ids": "keyword",
  "location": "geo_point",
  "embedding": "knn_vector",
  "is_active": "boolean",
  "document_version": "integer"
}
```

Phải hỗ trợ BM25, vector KNN, metadata filter, time filter, entity filter, geo filter và hybrid score fusion.

---
## 13. Hybrid RAG online flow

```mermaid
sequenceDiagram
    participant U as User
    participant API as FastAPI
    participant ORCH as RAG Orchestrator
    participant C as Redis
    participant OS as OpenSearch
    participant DB as PostgreSQL
    participant K as Kafka/Flink Read Models
    participant LLM as LLM Gateway
    participant VAL as Validators

    U->>API: Ask question
    API->>ORCH: request_id + query + user context
    ORCH->>C: Cache lookup

    alt Cache hit
        C-->>ORCH: validated response
    else Cache miss
        ORCH->>ORCH: Query classification
        par Document retrieval
            ORCH->>OS: Hybrid search
            OS-->>ORCH: ranked chunks
        and Structured retrieval
            ORCH->>DB: shipment/inventory query
            DB-->>ORCH: structured records
        and Current risk state
            ORCH->>K: alert/risk state lookup
            K-->>ORCH: current risk data
        end
        ORCH->>ORCH: Context assembly
        ORCH->>LLM: grounded prompt
        LLM-->>ORCH: raw answer
        ORCH->>VAL: validate
        VAL-->>ORCH: approved/retry/blocked
        ORCH->>C: Cache approved result
    end

    ORCH-->>API: response + citations
    API-->>U: final answer
```

---

## 14. Query router

Intent:

```text
DOCUMENT_SEARCH
STRUCTURED_SQL
CURRENT_ALERTS
GEOSPATIAL
RISK_EXPLANATION
SUPPLIER_DEPENDENCY
COMPOSITE
```

| Query | Route |
|---|---|
| Chính sách đánh giá supplier là gì? | DOCUMENT_SEARCH |
| Shipment nào tới muộn tuần này? | STRUCTURED_SQL |
| Có alert critical nào đang mở? | CURRENT_ALERTS |
| Facility nào gần trận động đất nhất? | GEOSPATIAL |
| Vì sao supplier ABC bị high risk? | RISK_EXPLANATION |
| Rủi ro hiện tại ảnh hưởng những PO nào? | COMPOSITE |

MVP:

- Rule + keyword + small classifier.
- Không dùng unrestricted agent.

Advanced:

- Fine-tuned intent classifier.
- Structured LLM output.
- Confidence threshold.
- Fallback route.

---

## 15. Retrieval

### 15.1. Document retrieval

```text
final_score =
    alpha × normalized_vector_score
  + beta × normalized_bm25_score
  + gamma × recency_score
  + delta × source_reliability
```

MVP:

```text
alpha = 0.55
beta  = 0.30
gamma = 0.10
delta = 0.05
```

Sau retrieval:

1. Metadata filtering.
2. Deduplicate cùng document.
3. Optional MMR.
4. Optional reranking.
5. Token-budget selection.

### 15.2. Structured retrieval

Không cho LLM tự chạy SQL tùy ý. Dùng tools có schema rõ:

```python
get_affected_shipments(
    event_id: str,
    risk_level: str | None,
    limit: int = 50
)
```

```python
get_inventory_exposure(
    supplier_id: str,
    product_ids: list[str] | None
)
```

```python
get_open_purchase_orders(
    supplier_id: str,
    required_before: date | None
)
```

### 15.3. Geospatial retrieval

```python
find_facilities_near_event(event_id: str, radius_km: float)
```

```python
find_events_near_facility(facility_id: str, since: datetime)
```

---

## 16. Prompt assembly

Prompt gồm:

```text
SYSTEM RULES
USER CONTEXT
QUESTION
CURRENT RISK STATE
STRUCTURED BUSINESS DATA
RETRIEVED DOCUMENT EVIDENCE
OUTPUT SCHEMA
```

Quy tắc:

- Không tự tạo shipment ID.
- Không suy diễn facility đã đóng cửa nếu nguồn không nói rõ.
- Phân biệt confirmed impact và potential impact.
- Mọi kết luận quan trọng phải có citation.
- Nếu thiếu dữ liệu, trả lời rõ phần chưa xác định.
- Số liệu lấy từ structured context.

Output JSON nội bộ:

```json
{
  "answer": "...",
  "claims": [
    {
      "claim": "...",
      "evidence_ids": ["chunk-1", "shipment-2"],
      "confidence": 0.87
    }
  ],
  "affected_entities": [
    {
      "entity_type": "shipment",
      "entity_id": "..."
    }
  ],
  "limitations": []
}
```

---

## 17. Post-processing

### 17.1. Citation validator

- `evidence_id` tồn tại.
- Evidence thuộc request context.
- Claim có evidence.
- Không cite document inactive.

### 17.2. Identifier validator

- Shipment ID tồn tại.
- Supplier ID tồn tại.
- PO ID tồn tại.
- Event ID tồn tại.

### 17.3. Numerical consistency validator

So sánh:

- Số shipment trong answer.
- Risk score.
- Inventory days.
- Open order value.
- Khoảng cách địa lý.

### 17.4. Policy validator

Không cho phép:

- Khẳng định certainty khi chỉ có weak evidence.
- Trả dữ liệu ngoài tenant.
- Lộ PII.
- Thực hiện action thay người dùng.

### 17.5. Validation result

```json
{
  "request_id": "uuid",
  "status": "APPROVED",
  "violations": [],
  "validated_at": "...",
  "validator_version": "1.0.0"
}
```

Trạng thái:

```text
APPROVED
RETRY_WITH_REVISED_PROMPT
BLOCKED
NEEDS_HUMAN_REVIEW
FAILED
```

---

## 18. Flink jobs

Không xây dựng một job khổng lồ.

### Job 1 — `external-event-normalizer`

Input: `ext.*.raw.v1`  
Output: `events.normalized.v1`

- Parse.
- Normalize.
- Validate.
- Event-time extraction.
- Data quality.

### Job 2 — `event-deduplicator`

Input: `events.normalized.v1`  
Output: `events.deduplicated.v1`, `audit.data_quality.v1`

- Keyed state.
- Version comparison.
- Payload hash.
- State TTL.

### Job 3 — `operational-current-state`

Input: Debezium CDC topics.  
Output: `ops.*.current.v1`

- Unwrap Debezium envelope.
- Handle delete/tombstone.
- Compact state topics.
- Normalize timestamps.

### Job 4 — `event-geo-correlator`

Input: events + facilities + warehouses.  
Output: `risk.impact_candidates.v1`

- Haversine.
- Broadcast state hoặc temporal lookup.
- Threshold rules.
- Late-event handling.

### Job 5 — `risk-feature-builder`

Input: impact candidates + operational current state.  
Output: `risk.features.v1`

- Stateful joins.
- Aggregation.
- Inventory days calculation.
- Exposure calculation.

### Job 6 — `risk-scorer`

Input: `risk.features.v1`  
Output: `risk.scores.v1`

- Rule-based scoring trong MVP.
- Model inference ở giai đoạn sau.

### Job 7 — `alert-manager`

Input: risk scores + existing alerts + event lifecycle.  
Output: alerts topics.

- Dedup alert.
- Update severity.
- Close expired alerts.
- Escalation.

### Job 8 — `lakehouse-sink`

Input: canonical, risk và RAG audit topics.  
Output: Iceberg tables.

---

## 19. AI services

### 19.1. Event classifier

Input:

```json
{
  "title": "...",
  "description": "...",
  "content": "..."
}
```

Output:

```json
{
  "labels": [
    {"name": "factory_fire", "probability": 0.91}
  ],
  "model_version": "..."
}
```

MVP dùng zero-shot classifier, LLM structured extraction hoặc rule baseline. Sau đó tạo dataset và fine-tune.

### 19.2. NER

Entities:

- Organization.
- Facility.
- Port.
- City.
- Country.
- Product.
- Commodity.
- Event location.

### 19.3. Entity linking

```text
mention normalization
    → exact/alias matching
    → embedding candidate retrieval
    → reranking
    → threshold
    → canonical entity ID
```

```sql
CREATE TABLE supplier_aliases (
    alias TEXT NOT NULL,
    supplier_id UUID NOT NULL,
    alias_type TEXT NOT NULL,
    confidence NUMERIC(5,4),
    PRIMARY KEY (alias, supplier_id)
);
```

Output giữ mention, candidate list, selected entity, confidence và linking method.

### 19.4. Risk model

Training data từ simulated disruptions, human labels, historical alerts và synthetic counterfactual cases.

MLflow lưu:

- Parameters.
- Metrics.
- Feature schema.
- Training dataset version.
- Model artifact.
- SHAP summary.

---

## 20. API design

Base prefix:

```text
/api/v1
```

### Health

```http
GET /health/live
GET /health/ready
```

### Alerts

```http
GET /alerts
GET /alerts/{alert_id}
POST /alerts/{alert_id}/acknowledge
POST /alerts/{alert_id}/resolve
POST /alerts/{alert_id}/mark-false-positive
```

### Events

```http
GET /events
GET /events/{event_id}
GET /events/{event_id}/impacts
```

### Suppliers

```http
GET /suppliers
GET /suppliers/{supplier_id}
GET /suppliers/{supplier_id}/risk
GET /suppliers/{supplier_id}/shipments
```

### Facilities

```http
GET /facilities
GET /facilities/{facility_id}
GET /facilities/{facility_id}/nearby-events
```

### RAG

```http
POST /rag/query
GET /rag/query/{request_id}
GET /rag/query/{request_id}/trace
```

Request:

```json
{
  "query": "Những shipment nào có nguy cơ chậm do động đất tại Nhật Bản?",
  "conversation_id": "uuid",
  "response_mode": "accurate",
  "filters": {
    "risk_level": ["high", "critical"]
  }
}
```

Response:

```json
{
  "request_id": "uuid",
  "answer": "...",
  "citations": [
    {
      "evidence_id": "chunk-123",
      "title": "...",
      "source": "USGS",
      "published_at": "...",
      "url": "..."
    }
  ],
  "entities": [],
  "confidence": 0.87,
  "limitations": [],
  "latency_ms": 1840
}
```

Streaming:

```text
/ws/alerts
/ws/rag/{request_id}
```

---

## 21. Dashboard

### Global Risk Map

- Event marker.
- Facility.
- Warehouse.
- Risk radius.
- Severity.
- Active alert.

### Alert Center

- Risk level.
- Event.
- Supplier.
- Facility.
- Affected shipments.
- Inventory days.
- Score.
- Status.
- Updated time.

### Alert Detail

- Summary.
- Map.
- Evidence.
- Affected assets.
- Risk factors.
- Timeline.
- Analyst actions.

### RAG Copilot

- Answer.
- Citation cards.
- Structured entities.
- Tool calls.
- Confidence.
- Validation status.
- Latency breakdown.

### Data Platform Monitoring

- Kafka lag.
- Events/sec.
- Flink checkpoint health.
- DLQ rate.
- Data freshness.
- Model inference latency.

---
## 22. Repository structure

```text
sentinelchain/
├── README.md
├── PLAN.md
├── Makefile
├── .env.example
├── docker-compose.yml
├── pyproject.toml
├── package.json
│
├── docs/
│   ├── architecture/
│   ├── adr/
│   ├── data-contracts/
│   ├── runbooks/
│   └── diagrams/
│
├── infra/
│   ├── docker/
│   ├── terraform/
│   ├── kubernetes/
│   ├── helm/
│   └── monitoring/
│
├── schemas/
│   ├── avro/
│   ├── jsonschema/
│   └── protobuf/
│
├── services/
│   ├── ingestion-usgs/
│   ├── ingestion-gdacs/
│   ├── ingestion-gdelt/
│   ├── supply-chain-simulator/
│   ├── document-parser/
│   ├── chunking-service/
│   ├── embedding-service/
│   ├── nlp-enrichment-service/
│   ├── entity-resolution-service/
│   ├── rag-orchestrator/
│   ├── llm-gateway/
│   ├── validation-service/
│   └── api-gateway/
│
├── flink/
│   ├── sql/
│   ├── jobs/
│   │   ├── external-event-normalizer/
│   │   ├── event-deduplicator/
│   │   ├── operational-current-state/
│   │   ├── event-geo-correlator/
│   │   ├── risk-feature-builder/
│   │   ├── risk-scorer/
│   │   ├── alert-manager/
│   │   └── lakehouse-sink/
│   └── common/
│
├── ml/
│   ├── datasets/
│   ├── features/
│   ├── training/
│   ├── evaluation/
│   ├── inference/
│   └── notebooks/
│
├── backend/
│   ├── app/
│   └── tests/
│
├── frontend/
│   ├── src/
│   └── tests/
│
├── tests/
│   ├── integration/
│   ├── end_to_end/
│   ├── contract/
│   ├── load/
│   └── fixtures/
│
└── scripts/
    ├── bootstrap.sh
    ├── create-topics.sh
    ├── register-schemas.sh
    ├── seed-operational-data.sh
    └── run-demo-scenario.sh
```

---

## 23. Coding conventions

### Python

- Python 3.12.
- `ruff`.
- `mypy`.
- `pytest`.
- Pydantic v2.
- Structured logging.
- No global mutable state.
- Dependency injection.
- Timezone-aware datetime only.

### Java/Flink

- Java 21 hoặc version tương thích Flink được chọn.
- Maven multi-module.
- Checkstyle.
- Unit test cho operator.
- Testcontainers cho Kafka/Postgres.
- Không hard-code topic name.

### TypeScript

- Strict mode.
- ESLint.
- Vitest.
- React Query.
- Generated API client từ OpenAPI.

### General

Mọi service có:

- README.
- Dockerfile.
- Health endpoint.
- Metrics endpoint.
- Config schema.
- Unit tests.
- Integration tests.

Không commit secret. Mọi event schema được version hóa. Mọi external call có timeout, retry và circuit breaker.

---

## 24. Local Docker Compose

Services tối thiểu:

```text
kafka-1
kafka-2
kafka-3
schema-registry
kafka-connect
flink-jobmanager
flink-taskmanager
postgres
opensearch
redis
minio
mlflow
prometheus
grafana
otel-collector
backend
frontend
```

Optional:

```text
iceberg-rest
kafka-ui
opensearch-dashboards
```

Make targets:

```bash
make up
make down
make reset
make bootstrap
make test
make test-integration
make demo
make lint
make format
```

---

## 25. Configuration

`.env.example`:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
POSTGRES_DSN=postgresql://sentinel:sentinel@postgres:5432/sentinel
OPENSEARCH_URL=http://opensearch:9200
REDIS_URL=redis://redis:6379/0
MINIO_ENDPOINT=http://minio:9000
MLFLOW_TRACKING_URI=http://mlflow:5000

USGS_POLL_INTERVAL_SECONDS=60
GDACS_POLL_INTERVAL_SECONDS=300
GDELT_POLL_INTERVAL_SECONDS=900

EMBEDDING_PROVIDER=local
EMBEDDING_MODEL=BAAI/bge-m3
LLM_PROVIDER=openai-compatible
LLM_MODEL=...
LLM_API_BASE=...
LLM_API_KEY=...

RISK_CRITICAL_THRESHOLD=0.80
RISK_HIGH_THRESHOLD=0.60
RISK_MEDIUM_THRESHOLD=0.40
```

---

## 26. Observability

### Metrics

Ingestion:

- `source_poll_total`
- `source_poll_failure_total`
- `source_records_fetched_total`
- `source_records_produced_total`
- `source_poll_latency_seconds`
- `source_cursor_lag_seconds`

Kafka/Flink:

- Consumer lag.
- Produce error rate.
- Checkpoint duration/failure.
- Backpressure.
- Records in/out.
- Watermark lag.
- State size.

AI:

- Model request count.
- Model latency.
- Token usage.
- Embedding batch size.
- Entity linking confidence.
- LLM validation failure rate.

Business:

- Active alerts.
- Critical alerts.
- Mean time to detect.
- False-positive rate.
- Affected shipment count.
- Alert lead time.

### Tracing

Trace propagation qua HTTP headers, Kafka headers, LLM calls, OpenSearch và SQL. Trace ID xuất hiện trong logs, Kafka envelope, API response và RAG trace endpoint.

### Logging

```json
{
  "timestamp": "...",
  "level": "INFO",
  "service": "ingestion-usgs",
  "trace_id": "...",
  "event_id": "...",
  "message": "event_produced"
}
```

---

## 27. Security

MVP:

- Secret qua environment hoặc Docker secret.
- API key không commit.
- Input validation.
- Query timeout.
- Rate limiting.
- CORS explicit.
- Audit log.
- PII masking.

Production:

- OAuth2/OIDC.
- RBAC.
- Topic ACL.
- TLS/mTLS.
- OpenSearch document-level security.
- Tenant filter.
- Vault hoặc cloud secret manager.

RAG-specific:

- Prompt injection detection.
- Retrieved-content sanitization.
- Tool allowlist.
- Max tool-call depth.
- Structured output validation.
- No unrestricted SQL generation.

---

## 28. Data quality

### USGS

- Latitude trong `[-90, 90]`.
- Longitude trong `[-180, 180]`.
- Magnitude hợp lệ.
- Event time không quá xa tương lai.
- `source_event_id` không rỗng.

### Shipment

- ETA không trước planned departure.
- Status thuộc enum.
- Value không âm.
- Supplier và facility tồn tại.

### Inventory

- Quantity không âm.
- Average daily usage không âm.
- Warehouse/product tồn tại.

Invalid record:

```text
→ audit.data_quality.v1
→ optional DLQ
```

---

## 29. Testing strategy

### Unit tests

- Parser.
- Schema validation.
- Haversine.
- Risk score.
- Entity normalization.
- Chunking.
- Prompt builder.
- Validators.

### Contract tests

- Producer/consumer schema compatibility.
- OpenAPI compatibility.
- Kafka event versioning.
- Debezium unwrap mapping.

### Integration tests

Dùng Testcontainers cho Kafka, PostgreSQL, OpenSearch và Redis.

- Event produce → normalized topic.
- CDC row update → current-state topic.
- Earthquake near facility → impact candidate.
- Risk feature → alert.
- Document → chunks → OpenSearch.
- Query → retrieval → validated response.

### Flink tests

- Operator test harness.
- Watermark.
- Late event.
- State recovery.
- Duplicate update.
- Tombstone.
- Checkpoint/restart.

### End-to-end scenario

1. Seed supplier tại Nhật Bản.
2. Seed facility gần Tokyo.
3. Tạo 5 active shipments.
4. Inventory chỉ còn 3 ngày.
5. Produce earthquake magnitude 6.5 cách facility 30 km.
6. Flink tạo impact.
7. Risk score thành critical.
8. Alert xuất hiện trên API/UI.
9. RAG query trả đúng shipment IDs và citations.
10. Validator xác nhận số liệu.

### Load tests

- 1.000 events/s canonical stream.
- 100 RAG queries đồng thời.
- 10.000 active shipments.
- 100.000 documents.
- Measure p50/p95/p99.

---

## 30. Acceptance criteria

### Data Engineering

- Event đi từ source đến canonical topic trong p95 dưới 10 giây sau khi ingestion nhận được.
- Không tạo duplicate alert khi replay.
- Flink restart không làm mất state.
- CDC update xuất hiện trong current-state topic.
- DLQ hoạt động.

### Risk engine

- Earthquake gần facility tạo impact candidate.
- Event xa hơn threshold không tạo impact.
- Risk score giải thích được.
- Alert lifecycle hoạt động.

### RAG

- Query phức hợp trả được document evidence và structured data.
- Citation tồn tại.
- Không hallucinate shipment ID.
- Số lượng shipment khớp database.
- Response không approved nếu validator thất bại.

### Operations

- `docker compose up` khởi động hệ thống.
- `make demo` chạy scenario end-to-end.
- Grafana có dashboard cơ bản.
- README đủ để người khác chạy.

---

## 31. Milestones

### Milestone 0 — Repository bootstrap

- Monorepo.
- CI.
- Docker Compose skeleton.
- Makefile.
- Lint/test setup.
- ADR template.

Done khi CI xanh và `make up` chạy Kafka, Postgres, OpenSearch.

### Milestone 1 — Operational data và CDC

- PostgreSQL schema.
- Simulator.
- Debezium connector.
- CDC topics.
- Current-state Flink job.

Done khi update/delete trong Postgres xuất hiện đúng trên Kafka.

### Milestone 2 — External event ingestion

- USGS ingestion.
- GDACS ingestion.
- Cursor persistence.
- Raw schemas.
- Metrics.

Done khi live event xuất hiện trên Kafka và restart không tạo duplicate vô hạn.

### Milestone 3 — Normalization và geospatial correlation

- Normalizer.
- Deduplicator.
- Facility broadcast state.
- Haversine correlation.
- Impact candidate topic.

Done khi event gần facility tạo impact, event xa không tạo impact.

### Milestone 4 — Risk scoring và alerting

- Feature builder.
- Rule-based scorer.
- Alert manager.
- Alert API.
- Basic dashboard.

Done khi chạy được critical alert end-to-end.

### Milestone 5 — Document/RAG pipeline

- Document parser.
- Chunking service.
- Embedding service.
- OpenSearch index.
- Hybrid retrieval.
- RAG API.
- Citation validator.

Done khi query trả citation và structured shipment data.

### Milestone 6 — AI enrichment

- Event classifier.
- NER.
- Entity linking.
- Evaluation dataset.
- Metrics dashboard.

Done khi unknown entity không bị ép liên kết và có offline evaluation.

### Milestone 7 — ML risk model

- Training pipeline.
- Feature dataset.
- MLflow tracking.
- Online inference.
- SHAP explanation.

Done khi model versioned, so sánh baseline và rollback được.

### Milestone 8 — Production hardening

- Kubernetes.
- Helm.
- Terraform.
- Autoscaling.
- Security.
- Load test.
- Runbook.

Done khi deploy lên cluster và recovery test pass.

---

## 32. ADR bắt buộc

```text
ADR-001 Kafka serialization format
ADR-002 Flink SQL vs DataStream API
ADR-003 OpenSearch as hybrid retrieval store
ADR-004 Chunking outside Flink SQL
ADR-005 Rule-based risk model before ML
ADR-006 Event envelope and schema evolution
ADR-007 Idempotency strategy
ADR-008 RAG validation architecture
ADR-009 CDC current-state design
ADR-010 Lakehouse adoption phase
```

---

## 33. Idempotency và failure handling

### Producer

- Enable idempotence.
- Deterministic event key.
- Retry safe.

### External calls

Embedding, LLM và OpenSearch không thuộc Kafka exactly-once transaction.

```text
operation_id = hash(input_id + model_version + operation_type)
```

Lưu processed operation.

### Retry topics

```json
{
  "original_topic": "...",
  "original_event": {},
  "attempt": 3,
  "next_retry_at": "...",
  "last_error": "...",
  "error_type": "TRANSIENT"
}
```

### DLQ

DLQ giữ original payload, schema version, error summary, service, timestamp, retry count và trace ID.

---

## 34. Performance targets

| Metric | Target |
|---|---:|
| Source received → normalized | p95 < 5 s |
| Normalized → risk score | p95 < 10 s |
| Risk score → dashboard | p95 < 3 s |
| RAG response accurate mode | p95 < 8 s |
| RAG cache hit | p95 < 500 ms |
| Embedding throughput | >= 100 chunks/s local batch |
| Duplicate alert rate | < 1% |
| Unsupported claim rate | < 5% trên test set |

---

## 35. Demo scenario

```bash
make demo
```

Steps:

1. Reset database.
2. Seed supplier/facility ở Nhật.
3. Seed products, PO, shipments và inventory.
4. Start ingestion và Flink jobs.
5. Inject deterministic earthquake event.
6. Wait until alert created.
7. Query API.
8. Query RAG.
9. Print citations và validation result.
10. Open dashboard.

Expected:

```text
CRITICAL ALERT
Supplier: JP Semiconductor Components
Facility: Yokohama Plant
Distance: 31.8 km
Affected shipments: 5
Inventory remaining: 3.1 days
Risk score: 0.91
```

---

## 36. Hướng dẫn cho coding agent

1. Triển khai theo milestone, không xây toàn bộ cùng lúc.
2. Trước mỗi milestone:
   - Đọc `PLAN.md`.
   - Tạo task checklist.
   - Xác định files cần thay đổi.
   - Viết tests trước hoặc cùng implementation.
3. Không thêm công nghệ mới nếu không có ADR.
4. Không hard-code secret, topic, URL hoặc threshold.
5. Không để Kafka consumer xử lý message mà không có error strategy.
6. Không dùng LLM để thay thế deterministic business logic.
7. Không cho LLM sinh SQL tùy ý.
8. Mọi output LLM phải parse bằng schema.
9. Mọi service phải có health, metrics và structured logs.
10. Mọi Kafka schema phải đăng ký và kiểm tra compatibility.
11. Mọi datetime phải là UTC.
12. Mọi API list endpoint phải phân trang.
13. Mọi external call phải có timeout.
14. Mọi PR phải kèm tests, docs, migration notes và observability changes.
15. Không đánh dấu milestone hoàn thành nếu chưa chạy end-to-end test.

---

## 37. Thứ tự triển khai khuyến nghị

```text
1. Kafka + Postgres + Debezium
2. Supply chain simulator
3. USGS ingestion
4. Flink normalize/dedup
5. Geospatial correlation
6. Risk feature builder
7. Rule-based score
8. Alerts API + dashboard
9. GDELT/GDACS
10. Document chunking + embeddings
11. Hybrid RAG
12. Validators
13. NLP entity linking
14. ML risk model
15. Iceberg + Kubernetes
```

Không bắt đầu bằng agent hoặc LLM trước khi data flow cơ bản hoạt động.

---

## 38. Tiêu chí portfolio

README cuối cùng phải có:

- Problem statement.
- Architecture diagram.
- Live data sources.
- Data contracts.
- Kafka topic map.
- Flink job map.
- AI/ML components.
- RAG flow.
- Demo GIF/video.
- Performance metrics.
- Offline model metrics.
- Failure/recovery demo.
- Trade-offs.
- Future work.

CV description:

```text
Built an event-driven supply-chain risk intelligence platform using Kafka,
Flink, Debezium CDC, OpenSearch, geospatial stream processing, online ML
inference, and hybrid RAG to correlate live global events with suppliers,
shipments, inventory, and operational dependencies in near real time.
```

---

## 39. Kết luận kiến trúc

SentinelChain không phải một chatbot bọc quanh vector database.

```text
REAL-TIME RISK PIPELINE
Live events + CDC
    → Kafka
    → Flink
    → Geospatial + stateful joins
    → Features
    → Risk score
    → Alerts

HYBRID RAG PIPELINE
Documents + operational state + risk outputs
    → Chunking + embeddings
    → Hybrid retrieval
    → Structured tools
    → LLM
    → Validation
    → Grounded response
```

Nguyên tắc:

- Kafka là event backbone.
- Flink xử lý state, thời gian, join và orchestration.
- Chunking nằm trong service chuyên biệt.
- Vector store chỉ dùng cho semantic retrieval.
- Dữ liệu định lượng lấy từ structured source.
- LLM dùng để tổng hợp và giải thích, không thay thế logic nghiệp vụ.
- Mọi câu trả lời quan trọng phải có evidence và validation.
