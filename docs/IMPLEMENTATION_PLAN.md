# SentinelChain — Kế hoạch triển khai chi tiết

> Tài liệu này phân tích sâu dự án trong [`PLAN.md`](../PLAN.md) và đưa ra kế hoạch triển khai
> theo từng giai đoạn, kèm cấu trúc thư mục có chú giải. Dùng làm tài liệu tham chiếu
> trong suốt quá trình code.
>
> **Định hướng đã chốt:** giữ tính *enterprise* tối đa, chỉ đơn giản hóa những chỗ
> **không làm giảm giá trị** thể hiện năng lực; LLM dùng qua lớp `LLMProvider` trừu tượng
> để cắm provider sau.

---

## Mục lục

- [A. Phân tích dự án](#a-phân-tích-dự-án)
- [B. Quyết định nền tảng (ADR cần chốt trước)](#b-quyết-định-nền-tảng-adr-cần-chốt-trước)
- [C. Nguyên tắc "đơn giản hóa thông minh"](#c-nguyên-tắc-đơn-giản-hóa-thông-minh)
- [D. Lớp LLMProvider abstraction](#d-lớp-llmprovider-abstraction)
- [E. Lộ trình triển khai theo giai đoạn](#e-lộ-trình-triển-khai-theo-giai-đoạn)
- [F. Đường găng & rủi ro](#f-đường-găng--rủi-ro)
- [G. Cấu trúc thư mục dự án (có chú giải)](#g-cấu-trúc-thư-mục-dự-án-có-chú-giải)
- [H. Checklist bắt đầu (Phase 0)](#h-checklist-bắt-đầu-phase-0)

---

## A. Phân tích dự án

### A.1. Bản chất hệ thống

SentinelChain **không phải** một app CRUD hay chatbot RAG. Nó là **hai pipeline ghép lại
qua một backbone Kafka chung**:

1. **Real-time risk pipeline** (deterministic): Live events + CDC → Kafka → Flink
   (state/time/join/geo) → features → risk score → alerts. Đây là "xương sống" và là phần
   tạo giá trị portfolio thật sự (streaming + data engineering).
2. **Hybrid RAG pipeline** (probabilistic, được "neo" bằng dữ liệu deterministic):
   documents + operational state + risk output → chunking/embedding → hybrid retrieval +
   structured tools → LLM → validation → grounded answer.

Nguyên tắc kiến trúc cốt lõi (PLAN mục 39): **LLM chỉ tổng hợp/giải thích, không thay logic
nghiệp vụ; mọi con số lấy từ structured source; mọi kết luận quan trọng phải có evidence +
validation.** Đây là điểm phân biệt với "chatbot bọc vector DB" và phải giữ vững xuyên suốt.

### A.2. Đâu là phần KHÓ thật sự

| Hạng mục | Vì sao khó | Hệ quả |
|---|---|---|
| **Flink stateful jobs** (Job 2,4,5,7) | Keyed state, watermark, late event, dedup theo version, broadcast state, checkpoint/restart không mất state | Chiếm ~40% công sức; dễ sai, khó test nhất |
| **Idempotency end-to-end** | Ingestion cursor, idempotent producer, external call (embedding/LLM/OpenSearch) ngoài transaction Kafka | "Replay không tạo duplicate alert" là acceptance cứng |
| **RAG validation layer** | Citation/identifier/numerical/policy validators + lifecycle APPROVED/RETRY/BLOCKED | Phần "đắt giá" nhất của RAG, dễ làm hời hợt |
| **CDC current-state** (Job 3) | Unwrap Debezium envelope, xử lý delete/tombstone, compacted topic | Input cho mọi join phía sau |
| **Geospatial correlation** (Job 4) | Join event stream với facility state thay đổi theo thời gian (temporal/broadcast) | Trái tim của "risk" |

Phần "dễ" hơn nhưng tốn thời gian: ingestion services, simulator, FastAPI, dashboard.

### A.3. Đánh giá phạm vi thực tế

PLAN mô tả hệ thống **cấp doanh nghiệp đầy đủ** (Kafka 3 broker, Flink, Iceberg, K8s, Helm,
Terraform, MLflow, OTel). Làm đúng từng chữ là khối lượng **nhiều tháng full-time** và chạy
nặng trên một máy. → Chiến lược: **"vertical slice trước, chiều rộng sau"** — ưu tiên một
luồng xuyên suốt mỏng nhưng chạy thật (1 nguồn → Kafka → 1 Flink job → alert → API), rồi đắp
dày. Khớp PLAN mục 36.1 và mục 37.

### A.4. Điểm cần chốt trước khi code

- **Kafka 3 broker trên 1 máy** lãng phí cho dev → cho phép 1 broker (KRaft) ở `dev` profile.
- **Serialization** (ADR-001): chọn **Avro + Schema Registry** (đúng production-like, thể hiện
  schema evolution).
- **Flink SQL vs DataStream** (ADR-002): job stateful phức tạp dùng **DataStream/Java**, job
  đơn giản dùng SQL.
- **LLM provider**: dùng lớp trừu tượng, mặc định có `MockProvider` cho CI; provider thật cắm
  qua env.

---

## B. Quyết định nền tảng (ADR cần chốt trước)

| ADR | Quyết định | Lý do |
|---|---|---|
| ADR-001 Serialization | **Avro + Schema Registry** mọi topic | Schema evolution + contract test — điểm cộng portfolio |
| ADR-002 Flink | **DataStream/Java** cho job stateful, SQL cho job đơn giản | Dedup theo version & broadcast geo cần keyed/broadcast state |
| ADR-005 Risk model | Rule-based trước ML | Giảm rủi ro, có baseline |
| ADR-006 Envelope | Envelope PLAN mục 9, version bằng `event_version` | Đã định nghĩa sẵn |
| ADR-007 Idempotency | Producer idempotence + deterministic key + `operation_id` hash cho external calls | Acceptance criteria yêu cầu |
| ADR-009 CDC | Compacted current-state topic + Flink unwrap | Input cho mọi join |

Các ADR còn lại (003 OpenSearch, 004 chunking ngoài Flink, 008 RAG validation, 010 Iceberg
phase) viết khi đến giai đoạn tương ứng.

---

## C. Nguyên tắc "đơn giản hóa thông minh"

Triết lý: **không cắt tầng nào ra khỏi kiến trúc; chỉ giảm "số lượng/quy mô" ở chỗ chỉ tốn
tài nguyên.** Mọi capability trong PLAN đều xuất hiện ít nhất ở mức "1 instance chạy thật +
documented".

| Hạng mục | Quyết định | Lý do |
|---|---|---|
| Kafka broker | Thiết kế cluster, **1 broker KRaft ở `dev`, 3 broker ở `prod`/CI-integration** | Chỉ giảm RAM, không giảm giá trị |
| Avro + Schema Registry | **GIỮ FULL** | Điểm cộng lớn |
| Flink stateful jobs | **GIỮ FULL, DataStream/Java** | Lõi data engineering |
| Debezium CDC | **GIỮ FULL** | Chữ ký kỹ thuật dự án |
| RAG validation (4 validators) | **GIỮ FULL** | Phân biệt với "chatbot wrap vector DB" |
| MLflow + ML model | **Giữ**, dataset từ simulator/synthetic | Không có data thật → synthetic hợp lý |
| OpenSearch hybrid (BM25+kNN+geo) | **GIỮ FULL** | Core RAG retrieval |
| Observability | **Giữ**, dashboard tối thiểu nhưng thật, cắm sớm từ `libs/common` | |
| Iceberg lakehouse-sink (Job 8) | **1 job + vài bảng**, không full medallion | Đủ thể hiện năng lực |
| Kubernetes/Helm | **Helm chart 2–3 service trọng yếu + manifest mẫu** | Chứng minh năng lực, không sa lầy |
| Terraform | **1 module mẫu** (kind cluster hoặc managed resource) | Thể hiện, không tốn cloud |
| Multi-tenant/OAuth2/mTLS | **`prod` profile + implementation mỏng + ADR** | Production concern minh họa |
| Load test | **Target nhỏ hơn** (vd 200 events/s), đo p50/p95/p99 thật | Scale xuống cho máy dev |

---

## D. Lớp LLMProvider abstraction

Đặt interface ở `libs/common`, implementation ở `services/llm-gateway`.

### D.1. Kiến trúc

```
rag-orchestrator
      │ (HTTP nội bộ, schema cố định)
      ▼
 llm-gateway  ──►  LLMProvider (interface)
                     ├── AnthropicProvider        (claude-opus-4-8, claude-sonnet-4-6, ...)
                     ├── OpenAICompatibleProvider  (OpenAI / vLLM / together / ...)
                     ├── VLLMProvider              (local, khi có GPU)
                     └── MockProvider              (deterministic, cho test/CI)
```

### D.2. Interface (Python, Pydantic v2)

```python
class LLMRequest(BaseModel):
    request_id: str
    messages: list[ChatMessage]
    response_schema: dict | None      # JSON schema bắt buộc parse được (PLAN 36.8)
    max_tokens: int
    temperature: float = 0.0
    tools: list[ToolSpec] | None      # tool allowlist (PLAN 27)
    trace_id: str

class LLMResponse(BaseModel):
    request_id: str
    content: str
    parsed: dict | None               # output đã validate theo response_schema
    usage: TokenUsage                  # cho metric token_usage (PLAN 26)
    model: str
    finish_reason: str
    latency_ms: int

class LLMProvider(Protocol):
    async def generate(self, req: LLMRequest) -> LLMResponse: ...
    async def health(self) -> bool: ...
    @property
    def capabilities(self) -> ProviderCapabilities: ...   # supports_tools, json_mode, max_context...
```

### D.3. Concern chung do gateway bọc một lần (provider chỉ lo gọi model)

- Timeout + retry exponential backoff + circuit breaker (PLAN 23, 36.13).
- Idempotency: `operation_id = hash(request_id + model_version + "llm")` (PLAN 33) → cache,
  replay an toàn.
- Rate limiting + token budgeting.
- Structured output enforcement: provider không có native JSON mode → prompt + parser +
  repair loop; fail → `finish_reason=schema_violation` → orchestrator đẩy
  `rag.responses.retry.v1`.
- OTel tracing xuyên LLM call (trace_id trong header).
- Metrics: model request count, latency, token usage, validation failure rate.
- Provider selection qua config (`LLM_PROVIDER` env) — không hard-code.

### D.4. Lợi ích

- CI dùng `MockProvider` → test RAG e2e **không tốn API, deterministic**.
- Đổi Anthropic ↔ OpenAI ↔ vLLM chỉ bằng env, không sửa orchestrator.
- Tách "business logic RAG" khỏi "chi tiết provider" (đúng PLAN mục 39).

> Model id khi cắm Anthropic: `claude-opus-4-8` (Opus 4.8), `claude-sonnet-4-6`,
> `claude-haiku-4-5-20251001` — chọn theo latency/chi phí, cấu hình qua env, không cứng trong code.

---

## E. Lộ trình triển khai theo giai đoạn

Giữ đánh số Milestone của PLAN (M0–M8), bám thứ tự PLAN mục 37.

### Phase 0 / M0 — Repository bootstrap

**Mục tiêu:** khung repo + hạ tầng local chạy được, CI xanh.

1. Cấu trúc thư mục monorepo (xem [mục G](#g-cấu-trúc-thư-mục-dự-án-có-chú-giải)).
2. `pyproject.toml` (uv/poetry), Python 3.12, cấu hình `ruff` + `mypy` + `pytest`. Thư viện
   chung `libs/common`: event envelope (Pydantic v2), structured logging, config loader,
   Kafka producer/consumer wrapper, OTel setup, health/metrics helper, `LLMProvider` interface.
   **Đầu tư kỹ — dùng lại cho mọi service.**
3. `docker-compose.yml` với **profiles**: `core` (kafka-1 KRaft, schema-registry, postgres,
   opensearch, redis), `full` (thêm flink, minio, mlflow, prometheus, grafana, otel-collector).
4. `Makefile` targets (PLAN mục 24): `up/down/reset/bootstrap/test/demo/lint/format`.
5. `scripts/`: `bootstrap.sh`, `create-topics.sh`, `register-schemas.sh`.
6. GitHub Actions: lint + mypy + unit test + build images. ADR template trong `docs/adr/`.
7. `git init` + `.gitignore` + `.gitattributes` (LF cho `*.sh`) + `.env.example` (PLAN 25).

**Done:** `make up` (profile core) khởi động Kafka/Postgres/OpenSearch; CI xanh; `libs/common`
có test.

**Gotcha Windows:** dùng Bash tool cho shell scripts; `.gitattributes` ép LF; chú ý đường dẫn
volume trong compose.

### Phase 1 / M1 — Operational data + CDC

**Mục tiêu:** Postgres schema + simulator + Debezium → CDC topics → Flink current-state.

1. **DB schema** (PLAN mục 8): migrations bằng Alembic — suppliers, facilities, products,
   warehouses, routes, purchase_orders, shipments, shipment_events, inventory,
   supplier_aliases. Bật `REPLICA IDENTITY FULL` cho bảng CDC + `wal_level=logical`.
2. **Simulator** (`services/supply-chain-simulator`): chạy liên tục, mô phỏng hành vi PLAN 7.6
   (tạo PO, đổi shipment status, ETA drift, inventory tăng/giảm, delay, facility pause). Hỗ
   trợ seed deterministic (cho demo) + random liên tục. Tốc độ cấu hình qua env.
3. **Debezium** qua Kafka Connect, connector cho 5 bảng → `ops.public.*`.
4. **Flink Job 3 `operational-current-state`**: unwrap envelope Debezium, xử lý `op=c/u/d` +
   tombstone, ghi `ops.*.current.v1` (compacted). Job đầu tiên — làm kỹ làm mẫu.

**Done:** UPDATE/DELETE row trong Postgres → xuất hiện đúng trên `ops.*.current.v1`. Test
integration bằng Testcontainers.

**Gotcha:** delete → tombstone (value null) trên compacted topic, test riêng. Timestamp
Debezium là epoch micro → normalize sang UTC ISO.

### Phase 2 / M2 — External event ingestion (USGS trước)

**Mục tiêu:** ingestion service có cursor, idempotent, metrics — bắt đầu chỉ với **USGS**.

1. `services/ingestion-usgs`: poll feed GeoJSON theo `USGS_POLL_INTERVAL_SECONDS`.
2. **Cursor persistence**: lưu `last_seen updated_time` per source (Redis/Postgres). Chỉ emit
   khi `source_version` đổi hoặc `payload_hash` đổi.
3. **Idempotent producer**, key = `source_event_id`, vào `ext.usgs.raw.v1`. **Không commit
   cursor trước khi Kafka ack** (PLAN 11.1).
4. Retry exponential backoff + circuit breaker (helper từ `libs/common`).
5. Metrics (PLAN 26): `source_poll_total`, `source_records_produced_total`,
   `source_cursor_lag_seconds`... Health + structured logs có `trace_id`.
6. Data quality (PLAN 28 USGS): lat/lon/magnitude/time range.

**Done:** live earthquake lên Kafka; **restart không tạo duplicate vô hạn**.

> GDACS/GDELT làm sau (Phase 5) — không làm cùng lúc.

### Phase 3 / M3 — Normalization + dedup + geospatial correlation (lõi kỹ thuật)

1. **Job 1 `external-event-normalizer`**: `ext.*.raw.v1` → `events.normalized.v1`. Parse,
   chuẩn hóa schema chung, event-time extraction, data quality → invalid đẩy
   `audit.data_quality.v1`.
2. **Job 2 `event-deduplicator`**: keyed state theo `source+source_event_id`, lưu
   `{latest_source_version, payload_hash, last_seen_at}`, state TTL theo source. Logic PLAN
   11.2 (emit/drop/update/tombstone) → `events.deduplicated.v1`.
3. **Job 4 `event-geo-correlator`**: facility/warehouse current-state vào **broadcast state**;
   mỗi event tính Haversine, áp threshold PLAN 11.4 (mag≥7→250km, ≥6→120km, ≥5→50km) →
   `risk.impact_candidates.v1`. Xử lý late event (watermark + allowed lateness).

**Done:** event gần facility (≤ threshold) tạo impact; xa hơn **không** tạo impact. Operator
test harness + watermark/late-event test.

**Gotcha:** broadcast state phải seed trước khi event đến; facility update giữa chừng →
broadcast cập nhật; Haversine có unit test với điểm đã biết.

### Phase 4 / M4 — Risk feature + scoring + alerting + API/dashboard (MVP demo-able)

1. **Job 5 `risk-feature-builder`**: join impact với operational current-state
   (facility→supplier→open POs→active shipments→destination inventory→product criticality).
   Tính `inventory_days_remaining`, `open_order_value`, `active_shipment_count` →
   `risk.features.v1` (PLAN 11.5).
2. **Job 6 `risk-scorer`**: rule-based (PLAN 11.6) + threshold → `risk.scores.v1` với
   `top_factors` (giải thích được).
3. **Job 7 `alert-manager`**: dedup theo `event_id+facility_id+alert_type`, lifecycle
   OPEN→ACK→INVESTIGATING→RESOLVED/FALSE_POSITIVE, close expired, escalation →
   `risk.alerts.v1` + `risk.alerts.current.v1` (compacted) + retry/dlq.
4. **Backend `api-gateway` (FastAPI)**: read-model (consume alerts.current vào Postgres
   read-model hoặc query trực tiếp), endpoints PLAN mục 20 + WebSocket `/ws/alerts`. Phân
   trang mọi list endpoint.
5. **Dashboard tối thiểu**: Alert Center + Alert Detail + Global Risk Map (MapLibre).

**Done:** earthquake gần facility → critical alert hiện trên API/UI; replay dup <1% (PLAN 34).

> **Cột mốc:** sau Phase 4 là điểm "MVP demo-able" thật sự (deterministic pipeline chạy thật).

### Phase 5 / M5 — Document/RAG pipeline (MVP)

1. **Thêm nguồn document**: GDELT ingestion → `rag.documents.raw.v1`; GDACS cho event.
2. **document-parser → chunking-service** (PLAN 12.4 MVP: recursive splitter 350–500 tokens,
   overlap 50–80, prefix title/heading) → `rag.documents.chunks.v1`.
3. **embedding-service**: batch embedding (bge-m3 local/API), idempotency theo
   `chunk_id+model_version`, lưu dimension/version → OpenSearch index (PLAN 12.6: knn_vector +
   BM25 + geo_point).
4. **rag-orchestrator**: query router (PLAN 14 MVP rule+keyword+small classifier) → hybrid
   retrieval (PLAN 15.1 fusion alpha/beta/gamma/delta) + structured tools schema cố định
   (PLAN 15.2, **không cho LLM tự sinh SQL**) → prompt assembly (PLAN 16) → **llm-gateway** →
   output JSON schema.
5. **POST /rag/query** + trace endpoint.

**Done:** query phức hợp trả document evidence + structured data; citation tồn tại; không
hallucinate shipment ID.

### Phase 6 / M5b — Validators (PLAN mục 17)

Citation / identifier / numerical consistency / policy validators → status
APPROVED/RETRY/BLOCKED/NEEDS_HUMAN_REVIEW. Response **không** trả về user nếu validator fail →
đẩy retry/dlq topic. Cache approved result vào Redis (PLAN 13).

**Done:** số shipment khớp DB; không cite document inactive; response bị chặn khi validator fail.

> **Cột mốc:** sau Phase 6 đã có cả hai pipeline (risk + RAG có validation) — trạng thái
> portfolio mạnh nhất với chi phí hợp lý.

### Phase 7 / M6 — AI enrichment (NLP)

Event classifier (zero-shot/LLM structured MVP) → NER → entity linking (PLAN 19.3, dùng
`supplier_aliases` + embedding candidate + threshold). Offline evaluation dataset.

**Done:** unknown entity **không bị ép link**; có offline metrics.

### Phase 8 / M7 — ML risk model

Feature dataset từ simulated disruptions + labels → LightGBM/XGBoost (baseline Logistic
Regression) → MLflow registry (params/metrics/feature schema/dataset version/SHAP) → online
inference thay rule-based trong Job 6. Targets PLAN 11.6.

**Done:** model versioned, so baseline, rollback được.

### Phase 9 / M8 — Production hardening (đại diện)

Iceberg lakehouse-sink (Job 8, vài bảng), Kubernetes manifest + Helm chart (2–3 service),
Terraform (1 module), security mỏng (OAuth2/RBAC/ACL/TLS minh họa), load test target nhỏ,
runbooks.

**Done:** deploy kind/cluster + recovery test pass.

### Bảng tổng hợp lộ trình

| Phase | Milestone | Sản phẩm chạy được | Done (kiểm chứng) |
|---|---|---|---|
| 0 | M0 | Repo + compose(profiles) + CI + libs/common | `make up` (core); CI xanh |
| 1 | M1 | Postgres + simulator + Debezium + Job 3 | UPDATE/DELETE → CDC current-state đúng |
| 2 | M2 | USGS ingestion | Live event lên Kafka; restart không dup |
| 3 | M3 | Job 1/2/4 (normalize, dedup, geo) | Gần→impact, xa→không; late-event pass |
| 4 | M4 | Job 5/6/7 + FastAPI + dashboard | Critical alert e2e; replay dup <1% |
| 5 | M5 | GDELT/GDACS + chunking + embedding + OpenSearch + RAG + llm-gateway | Query trả citation + structured data |
| 6 | M5b | 4 validators + cache + retry/dlq | Số liệu khớp DB; fail→blocked |
| 7 | M6 | Classifier + NER + entity linking + eval | Unknown entity không bị ép link |
| 8 | M7 | ML model + MLflow + inference + SHAP | Versioned, so baseline, rollback |
| 9 | M8 | Iceberg + Helm + Terraform + security + load test | Deploy + recovery test pass |

---

## F. Đường găng & rủi ro

```
M0 → M1 (CDC) → M3 (geo correlation) → M4 (alert e2e)   ← đường găng chính
              ↘ M2 (ingestion) ↗
M4 → M5 (RAG) → M6 (validators)
```

- **Rủi ro #1:** Flink stateful jobs (M3/M4). → viết operator test harness sớm; Job 3 làm kỹ
  làm mẫu.
- **Rủi ro #2:** tài nguyên máy. → compose profiles, 1 broker ở dev.
- **Rủi ro #3:** RAG validation bị làm hời hợt. → tách phase riêng (Phase 6), acceptance cứng.
- **Rủi ro #4:** scope creep. → dừng ở M4 đã có demo; M5–M8 tăng dần độ ấn tượng.

### Cross-cutting (mọi phase)

- Mỗi service: README, Dockerfile, health, metrics, config schema, unit + integration test.
- Mỗi Kafka schema: đăng ký + compatibility check (contract test).
- Mỗi PR: tests + docs + migration notes + observability changes.
- Không đánh dấu milestone xong nếu chưa chạy e2e test.
- Observability cắm sớm từ `libs/common`.

---

## G. Cấu trúc thư mục dự án (có chú giải)

Mở rộng từ PLAN mục 22, bổ sung `libs/` (thư viện chung) và chú giải vai trò từng phần.

```text
sentinelchain/
├── README.md                     # Problem statement, kiến trúc, cách chạy, demo (PLAN 38)
├── PLAN.md                       # Spec gốc
├── Makefile                      # up/down/reset/bootstrap/test/demo/lint/format
├── .env.example                  # Mẫu biến môi trường (PLAN 25)
├── .gitignore
├── .gitattributes                # Ép LF cho *.sh (chạy được trên Windows/Linux)
├── docker-compose.yml            # Profiles: core | full
├── pyproject.toml                # Python 3.12, ruff, mypy, pytest, dependency groups
├── package.json                  # Workspace cho frontend (+ tooling JS dùng chung)
│
├── docs/                         # Tài liệu
│   ├── IMPLEMENTATION_PLAN.md    # (file này)
│   ├── architecture/             # Sơ đồ kiến trúc, mô tả pipeline
│   ├── adr/                      # Architecture Decision Records (ADR-001..010)
│   ├── data-contracts/           # Hợp đồng dữ liệu giữa service (schema + ví dụ)
│   ├── runbooks/                 # Quy trình vận hành/sự cố (DLQ, restart Flink...)
│   └── diagrams/                 # Mermaid/PNG nguồn
│
├── infra/                        # Hạ tầng & vận hành
│   ├── docker/                   # Dockerfile gốc, image cơ sở dùng chung, init scripts
│   ├── terraform/                # 1 module mẫu provision hạ tầng
│   ├── kubernetes/               # Manifest mẫu (namespace, deployment, service)
│   ├── helm/                     # Helm chart cho 2-3 service trọng yếu
│   └── monitoring/               # Prometheus config, Grafana dashboards, OTel collector config
│
├── schemas/                      # Schema sự kiện, version hóa (nguồn sự thật)
│   ├── avro/                     # Avro schema cho mọi Kafka topic (ADR-001)
│   ├── jsonschema/               # JSON Schema cho API payload & LLM output schema
│   └── protobuf/                 # (tùy chọn) nếu cần grpc nội bộ
│
├── libs/                         # Thư viện chung (BỔ SUNG so với PLAN — nền tảng dùng lại)
│   ├── common/                   # Python: envelope, logging, config, kafka wrapper,
│   │                             #   OTel, health/metrics, LLMProvider interface, retry/CB
│   └── common-java/              # Java: tiện ích chung cho Flink jobs (envelope, serde, config)
│
├── services/                     # Microservices (Python, mỗi service: README/Dockerfile/
│   │                             #   health/metrics/config/tests)
│   ├── ingestion-usgs/           # Poll USGS GeoJSON → ext.usgs.raw.v1 (cursor, idempotent)
│   ├── ingestion-gdacs/          # Poll GDACS → ext.gdacs.raw.v1
│   ├── ingestion-gdelt/          # Poll/parse GDELT → ext.gdelt.raw.v1 + rag.documents.raw.v1
│   ├── supply-chain-simulator/   # Sinh dữ liệu nghiệp vụ liên tục vào PostgreSQL
│   ├── document-parser/          # HTML/PDF → document chuẩn hóa
│   ├── chunking-service/         # Document → chunks (recursive splitter, metadata)
│   ├── embedding-service/        # Chunks → embeddings (batch, idempotent) → OpenSearch
│   ├── nlp-enrichment-service/   # Event classification + NER
│   ├── entity-resolution-service/# Entity linking (alias + embedding candidate + threshold)
│   ├── rag-orchestrator/         # Query router + retrieval + prompt assembly + orchestration
│   ├── llm-gateway/              # LLMProvider implementations (Anthropic/OpenAI/vLLM/Mock)
│   ├── validation-service/       # 4 validators (citation/identifier/numerical/policy)
│   └── api-gateway/              # FastAPI: alerts/events/suppliers/facilities/rag + WS
│
├── flink/                        # Stream processing (Java/DataStream + SQL)
│   ├── sql/                      # Flink SQL cho job đơn giản (normalize, current-state)
│   ├── jobs/                     # Job stateful (DataStream/Java) — multi-module Maven
│   │   ├── external-event-normalizer/   # Job 1: raw → normalized
│   │   ├── event-deduplicator/          # Job 2: dedup theo version (keyed state, TTL)
│   │   ├── operational-current-state/   # Job 3: Debezium unwrap → compacted current-state
│   │   ├── event-geo-correlator/        # Job 4: Haversine + broadcast state → impact
│   │   ├── risk-feature-builder/        # Job 5: stateful join → features
│   │   ├── risk-scorer/                 # Job 6: rule-based → ML inference
│   │   ├── alert-manager/               # Job 7: dedup alert + lifecycle
│   │   └── lakehouse-sink/              # Job 8: canonical/risk/audit → Iceberg
│   └── common/                   # Serde Avro, config, test harness chung cho Flink
│
├── ml/                           # Machine Learning
│   ├── datasets/                 # Sinh/version dataset (từ simulator + synthetic)
│   ├── features/                 # Feature engineering (khớp risk.features.v1)
│   ├── training/                 # Pipeline train (LogReg baseline, LightGBM/XGBoost)
│   ├── evaluation/               # Offline eval, so sánh model version, SHAP
│   ├── inference/                # Online inference service (cho Job 6 gọi)
│   └── notebooks/                # Phân tích/khám phá
│
├── backend/                      # (nếu tách riêng khỏi services/api-gateway)
│   ├── app/                      #   — hoặc dùng services/api-gateway làm backend chính
│   └── tests/
│
├── frontend/                     # React + TypeScript dashboard
│   ├── src/                      # Risk Map, Alert Center/Detail, RAG Copilot, Monitoring
│   └── tests/                    # Vitest
│
├── tests/                        # Test cấp hệ thống
│   ├── integration/              # Testcontainers: Kafka/Postgres/OpenSearch/Redis
│   ├── end_to_end/               # Kịch bản e2e (PLAN 29: earthquake → alert → RAG)
│   ├── contract/                 # Producer/consumer schema compat, OpenAPI compat
│   ├── load/                     # Load test (target nhỏ, đo p50/p95/p99)
│   └── fixtures/                 # Dữ liệu mẫu deterministic (event, seed DB)
│
└── scripts/                      # Tự động hóa
    ├── bootstrap.sh              # Khởi tạo toàn bộ (topics + schemas + seed)
    ├── create-topics.sh          # Tạo Kafka topics (PLAN mục 10)
    ├── register-schemas.sh       # Đăng ký Avro schema vào Schema Registry
    ├── seed-operational-data.sh  # Seed supplier/facility/PO/shipment/inventory
    └── run-demo-scenario.sh      # Kịch bản demo (PLAN mục 35)
```

### Ghi chú thiết kế thư mục

- **`libs/common` là nền tảng quan trọng nhất** (bổ sung so với PLAN gốc): mọi service Python
  thừa hưởng envelope, logging, config, kafka wrapper, OTel, health/metrics, retry/circuit
  breaker, và `LLMProvider` interface. Tránh lặp code và đảm bảo nhất quán cross-cutting.
- **`schemas/` là nguồn sự thật** cho mọi event — đăng ký vào Schema Registry, dùng cho
  contract test. Không định nghĩa schema rải rác trong từng service.
- **`flink/jobs/*` tách thành module độc lập** (không một job khổng lồ — PLAN mục 18). Job
  stateful dùng DataStream/Java; `flink/sql/` cho job đơn giản.
- **`api-gateway` vs `backend/`**: nên hợp nhất — dùng `services/api-gateway` làm backend
  FastAPI chính; thư mục `backend/` giữ chỗ nếu sau này cần tách BFF riêng. Quyết định cụ thể
  ghi trong ADR khi tới Phase 4.
- **Mọi service tuân thủ PLAN mục 23**: README, Dockerfile, health endpoint, metrics endpoint,
  config schema, unit + integration tests.

---

## H. Checklist bắt đầu (Phase 0)

Khi bắt đầu code, thực hiện theo thứ tự:

1. `git init` + `.gitignore` + `.gitattributes` (ép LF cho `*.sh`).
2. Cấu trúc thư mục (mục G) + `libs/common` (envelope, logging, config, kafka wrapper, OTel,
   health/metrics, `LLMProvider` interface).
3. `docker-compose.yml` với profiles `core` / `full`.
4. `Makefile` + `scripts/bootstrap.sh|create-topics.sh|register-schemas.sh`.
5. CI GitHub Actions (lint + mypy + pytest + build).
6. `docs/adr/` + viết **ADR-001 / 002 / 006 / 007 / 009** (quyết định nền tảng).
7. `.env.example` (PLAN mục 25).

**Done Phase 0:** `make up` (profile core) khởi động Kafka/Postgres/OpenSearch; CI xanh;
`libs/common` có test.
