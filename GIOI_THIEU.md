# 🌲 Forest Health Monitoring Lakehouse — Tài Liệu Hệ Thống

> Hệ thống giám sát sức khỏe rừng thời gian thực trên nền tảng lakehouse hiện đại, kết hợp dữ liệu cảm biến IoT, ảnh vệ tinh Sentinel-2 thực tế, dữ liệu thời tiết và ghi chú thực địa để theo dõi toàn diện 1 ha rừng Keo tại tỉnh Quảng Nam, Việt Nam.

---

## Mục Lục

1. [Tổng Quan Hệ Thống](#1-tổng-quan-hệ-thống)
2. [Kiến Trúc Tổng Thể](#2-kiến-trúc-tổng-thể)
3. [Hạ Tầng & Dịch Vụ (15 Docker Services)](#3-hạ-tầng--dịch-vụ-15-docker-services)
4. [Bố Cục Khu Rừng](#4-bố-cục-khu-rừng)
5. [Pipeline Dữ Liệu — Kiến Trúc Medallion](#5-pipeline-dữ-liệu--kiến-trúc-medallion)
6. [Thu Thập Dữ Liệu](#6-thu-thập-dữ-liệu)
7. [Luồng Streaming Thời Gian Thực](#7-luồng-streaming-thời-gian-thực)
8. [Xử Lý & Làm Sạch Dữ Liệu (Silver Layer)](#8-xử-lý--làm-sạch-dữ-liệu-silver-layer)
9. [Tổng Hợp & Phân Tích (Gold Layer)](#9-tổng-hợp--phân-tích-gold-layer)
10. [Orchestration với Apache Airflow](#10-orchestration-với-apache-airflow)
11. [Dashboard Trực Quan](#11-dashboard-trực-quan)
12. [Cấu Trúc Thư Mục](#12-cấu-trúc-thư-mục)
13. [Hướng Dẫn Cài Đặt & Chạy](#13-hướng-dẫn-cài-đặt--chạy)
14. [Cổng Truy Cập & Thông Tin Đăng Nhập](#14-cổng-truy-cập--thông-tin-đăng-nhập)
15. [Chi Tiết Kỹ Thuật](#15-chi-tiết-kỹ-thuật)

---

## 1. Tổng Quan Hệ Thống

### Mục Tiêu

Hệ thống được xây dựng nhằm giám sát liên tục sức khỏe của khu rừng 1 ha thông qua:

- **75 cảm biến IoT** (nhiệt độ, độ ẩm, độ ẩm đất) phân bổ theo lưới 5×5 ô
- **Dữ liệu vệ tinh Sentinel-2 thực tế** từ Microsoft Planetary Computer — chỉ số NDVI (Normalized Difference Vegetation Index)
- **Dữ liệu thời tiết** từ Open-Meteo API theo thời gian thực
- **Ghi chú thực địa** (OCR) và **ảnh thực địa** mô phỏng do cán bộ kiểm lâm ghi nhận
- **Chỉ số FHI (Forest Health Index)** tổng hợp tất cả nguồn dữ liệu

### Phạm Vi Địa Lý

| Thông số | Giá trị |
|---|---|
| Vị trí trung tâm | 15.3350°N, 108.2528°E |
| Tỉnh | Quảng Nam, Việt Nam |
| Diện tích | 1 ha (100m × 100m) |
| Loại rừng | Rừng Keo (Acacia) |
| Phân chia | 25 ô (plot), mỗi ô 20m × 20m |

### Phạm Vi Thời Gian

| Thông số | Giá trị |
|---|---|
| Dữ liệu lịch sử từ | 2024-04-10 |
| Dữ liệu lịch sử đến | 2025-04-09 |
| Cập nhật tự động | Mỗi 5 ngày (Airflow batch) |
| Streaming thời gian thực | Mỗi 10 giây (75 cảm biến) |

---

## 2. Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        NGUỒN DỮ LIỆU                                    │
├─────────────┬──────────────┬────────────────┬───────────────────────────┤
│  75 Cảm biến│  Vệ tinh     │ Thời tiết      │ Ghi chú / Ảnh thực địa   │
│  IoT (real- │  Sentinel-2  │ Open-Meteo API │ (OCR Notes + Image Meta) │
│  time/10s)  │  Planetary   │                │                          │
│             │  Computer    │                │                          │
└──────┬──────┴──────┬───────┴────────┬───────┴────────────┬─────────────┘
       │             │                │                    │
       ▼             │                │                    │
┌──────────────┐     │                │                    │
│   KAFKA      │     │                │                    │
│  (Streaming) │     │                │                    │
│  Topic:      │     │                │                    │
│ sensor-      │     │                │                    │
│ readings     │     │                │                    │
└──────┬───────┘     │                │                    │
       │             │                │                    │
       ▼             ▼                ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw Data)                             │
│            MinIO S3  →  Delta Lake  →  s3://lakehouse/bronze/           │
│  sensor_raw  │  satellite_raw  │  weather_raw  │  ocr_raw │  image_raw  │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │ Spark Transform
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (Cleaned Data)                         │
│            MinIO S3  →  Delta Lake  →  s3://lakehouse/silver/           │
│  sensor_clean │ satellite_clean │ weather_clean │ ocr_processed │       │
│                         image_processed                                 │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │ Spark Aggregation
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER (Analytics-Ready)                       │
│            MinIO S3  →  Delta Lake  →  s3://lakehouse/gold/             │
│              feature_store  │  forest_health_index  │  plot_summary     │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                                   ▼
                        ┌──────────────────┐
                        │   STREAMLIT      │
                        │   DASHBOARD      │
                        │  (4 trang)       │
                        │  :8501           │
                        └──────────────────┘
```

### Stack Công Nghệ

| Thành phần | Công nghệ |
|---|---|
| Lưu trữ đối tượng | MinIO (S3-compatible) |
| Định dạng dữ liệu | Delta Lake (ACID transactions) |
| Xử lý batch | Apache Spark 3.x |
| Streaming | Apache Kafka + Spark Structured Streaming |
| Orchestration | Apache Airflow 2.9.3 |
| Metadata DB | PostgreSQL 15 |
| Dashboard | Streamlit |
| Container | Docker Compose |
| Nguồn vệ tinh | Microsoft Planetary Computer (Sentinel-2 L2A) |
| Nguồn thời tiết | Open-Meteo API |

---

## 3. Hạ Tầng & Dịch Vụ (15 Docker Services)

Toàn bộ hệ thống chạy trong môi trường Docker Compose với mạng nội bộ `lakehouse`.

### 3.1 Nhóm Streaming — Kafka

#### `zookeeper`
- **Image**: `confluentinc/cp-zookeeper:7.5.0`
- **Vai trò**: Điều phối và quản lý cluster Kafka
- **Cổng nội bộ**: 2181
- **Bộ nhớ**: 256 MB

#### `kafka`
- **Image**: `confluentinc/cp-kafka:7.5.0`
- **Vai trò**: Message broker cho luồng dữ liệu cảm biến thời gian thực
- **Cổng ngoài**: `9092` (kết nối từ máy host)
- **Cổng nội bộ**: `29092` (container-to-container)
- **Bộ nhớ**: 512 MB
- **Health check**: `kafka-topics --list`

#### `kafka-ui`
- **Image**: `provectuslabs/kafka-ui:latest`
- **Vai trò**: Giao diện web quản lý Kafka — xem topics, messages, consumers
- **Cổng**: `8080` → http://localhost:8080
- **Bộ nhớ**: 256 MB

### 3.2 Nhóm Lưu Trữ

#### `minio`
- **Image**: `minio/minio:RELEASE.2024-01-16T16-07-38Z`
- **Vai trò**: Object storage S3-compatible — lưu toàn bộ Delta Lake tables
- **Cổng API**: `9000`
- **Cổng Console**: `9001` → http://localhost:9001
- **Credentials**: `minioadmin / minioadmin`
- **Persistence**: Docker volume `minio-data`
- **Bộ nhớ**: 512 MB
- **Health check**: `mc ready /data`

#### `minio-init`
- **Vai trò**: Tự động tạo bucket `lakehouse` khi khởi động hệ thống lần đầu
- **Chạy once**: Tạo bucket → exit 0

#### `postgres`
- **Image**: `postgres:15`
- **Vai trò**: Lưu metadata Airflow (DAG runs, task instances, connections)
- **Credentials**: `airflow / airflow`, database `airflow`
- **Persistence**: Docker volume `postgres-data`
- **Bộ nhớ**: 256 MB

### 3.3 Nhóm Xử Lý — Spark

#### `spark-master`
- **Image**: `bitnami/spark:3.5`
- **Vai trò**: Spark master node — điều phối các Spark jobs
- **Cổng Web UI**: `8082` → http://localhost:8082
- **Địa chỉ**: `spark://spark-master:7077`
- **Bộ nhớ**: 512 MB

#### `spark-worker`
- **Image**: `bitnami/spark:3.5`
- **Vai trò**: Spark worker node — thực thi tính toán
- **Cổng Web UI**: `8081` → http://localhost:8081
- **Cấu hình**: 2 cores, 1 GB RAM
- **Bộ nhớ limit**: 1.5 GB

#### `spark` (on-demand)
- **Build**: Custom image (docker/spark/Dockerfile)
- **Vai trò**: Container để chạy ad-hoc Spark jobs
- **Cổng**: `4040` (Spark UI khi chạy)
- **Bộ nhớ**: 2 GB
- **Cách dùng**: `docker compose run --rm spark <lệnh>`

### 3.4 Nhóm Ứng Dụng

#### `app` (on-demand)
- **Build**: Custom image (docker/app/Dockerfile)
- **Vai trò**: Container chạy các Python scripts (ingestion, transform, validation)
- **Bộ nhớ**: 1 GB
- **Cách dùng**: `docker compose run --rm app <lệnh>`

#### `sensor-producer`
- **Build**: Dùng lại image `app`
- **Vai trò**: Chạy liên tục — sinh dữ liệu cảm biến IoT và gửi lên Kafka mỗi 10 giây
- **Bộ nhớ**: 256 MB
- **Module**: `src.generators.sensor_producer`

#### `dashboard`
- **Build**: Custom image (docker/dashboard/Dockerfile)
- **Vai trò**: Streamlit dashboard trực quan 4 trang
- **Cổng**: `8501` → http://localhost:8501
- **Bộ nhớ**: 1 GB
- **Restart**: `unless-stopped`

### 3.5 Nhóm Orchestration — Airflow

#### `airflow-init`
- **Build**: Custom image (docker/airflow/Dockerfile)
- **Vai trò**: Khởi tạo Airflow DB + tạo user admin một lần duy nhất
- **Chạy**: `airflow db migrate` → tạo user → exit

#### `airflow-webserver`
- **Build**: Custom image (docker/airflow/Dockerfile)
- **Vai trò**: Giao diện web Airflow — quản lý DAGs, xem logs
- **Cổng**: `8088` → http://localhost:8088
- **Credentials**: `admin / admin`
- **Bộ nhớ**: 512 MB

#### `airflow-scheduler`
- **Build**: Custom image (docker/airflow/Dockerfile)
- **Vai trò**: Lập lịch và thực thi DAGs
- **Bộ nhớ**: 1 GB
- **Executor**: LocalExecutor (chạy tasks trực tiếp, không cần worker riêng)

---

## 4. Bố Cục Khu Rừng

### Lưới 5×5 — 25 Ô Giám Sát

Khu rừng 100m × 100m được chia thành 25 ô (plot) theo lưới đều 5×5, mỗi ô có diện tích 20m × 20m.

```
CỘT:   01       02       03       04       05
     ┌────────┬────────┬────────┬────────┬────────┐
HÀng │plot_01 │plot_01 │plot_01 │plot_01 │plot_01 │
  01 │  _01   │  _02   │  _03   │  _04   │  _05   │
     ├────────┼────────┼────────┼────────┼────────┤
  02 │plot_02 │plot_02 │plot_02 │plot_02 │plot_02 │
     │  _01   │  _02   │  _03   │  _04   │  _05   │
     ├────────┼────────┼────────┼────────┼────────┤
  03 │plot_03 │plot_03 │plot_03 │plot_03 │plot_03 │
     │  _01   │  _02   │  _03   │  _04   │  _05   │
     ├────────┼────────┼────────┼────────┼────────┤
  04 │plot_04 │plot_04 │plot_04 │plot_04 │plot_04 │
     │  _01   │  _02   │  _03   │  _04   │  _05   │
     ├────────┼────────┼────────┼────────┼────────┤
  05 │plot_05 │plot_05 │plot_05 │plot_05 │plot_05 │
     │  _01   │  _02   │  _03   │  _04   │  _05   │
     └────────┴────────┴────────┴────────┴────────┘
                    ↑ Trung tâm: plot_03_03
              (15.3350°N, 108.2528°E)
```

### Cảm Biến

Mỗi ô có **3 cảm biến** đặt ở các vị trí hơi lệch nhau (cách nhau ~3m theo hướng N-S, ~2m theo E-W):

| Cảm biến | Đo lường | Đơn vị | Phạm vi hợp lệ |
|---|---|---|---|
| `temperature` | Nhiệt độ không khí 2m | °C | 10 – 45 |
| `humidity` | Độ ẩm tương đối | % | 30 – 100 |
| `soil_moisture` | Độ ẩm đất | m³/m³ | 0.05 – 0.80 |

**Tổng cộng**: 25 ô × 3 cảm biến = **75 cảm biến**, mỗi sensor ID có dạng `sensor_RR_CC_NN` (ví dụ: `sensor_01_03_02`).

### Tọa Độ Tính Toán

Hệ thống tính tọa độ chính xác theo xấp xỉ tuyến tính:

```
DEG_PER_M_LAT = 1 / 111,320  ≈ 8.983e-6 °/m
DEG_PER_M_LON = 1 / (111,320 × cos(15.335°))  ≈ 9.299e-6 °/m
```

---

## 5. Pipeline Dữ Liệu — Kiến Trúc Medallion

Hệ thống áp dụng kiến trúc **Medallion (Bronze → Silver → Gold)** — tiêu chuẩn của Delta Lake / Databricks:

### Bronze Layer — Dữ Liệu Thô

Đường dẫn: `s3://lakehouse/bronze/`

| Bảng | Mô tả | Số dòng (ước tính) | Partition |
|---|---|---|---|
| `sensor_raw` | Readings thô từ Kafka/historical | ~7.9 triệu | `year_month` |
| `satellite_raw` | Dữ liệu NDVI thô từ Planetary Computer | ~575 | `year_month` |
| `weather_raw` | Dữ liệu thời tiết thô từ Open-Meteo | ~8,760+ | `year_month` |
| `ocr_raw` | Ghi chú thực địa OCR | ~4,500 | `year_month` |
| `image_raw` | Metadata ảnh thực địa | ~2,500 | `year_month` |

### Silver Layer — Dữ Liệu Đã Làm Sạch

Đường dẫn: `s3://lakehouse/silver/`

| Bảng | Mô tả | Thao tác làm sạch |
|---|---|---|
| `sensor_clean` | Cảm biến đã chuẩn hóa | Loại NaN, clip outliers, thêm cờ bất thường, parse timestamp |
| `satellite_clean` | NDVI đã enriched | Tính NDVI rolling 30 ngày, phát hiện bất thường, thêm year_month |
| `weather_clean` | Thời tiết đã chuẩn hóa | Loại duplicate, kiểm tra phạm vi hợp lệ |
| `ocr_processed` | Ghi chú đã phân loại | Phát hiện từ khóa bệnh, hạn hán, dịch hại |
| `image_processed` | Metadata ảnh đã xử lý | Trích xuất labels, severity scores |

**Quy tắc làm sạch cảm biến:**
- Nhiệt độ: clip [-10°C, 60°C], đánh dấu bất thường nếu > 40°C hoặc < 5°C
- Độ ẩm: clip [0%, 100%]
- Độ ẩm đất: clip [0.0, 1.0], đánh dấu khô hạn nếu < 0.15

### Gold Layer — Dữ Liệu Sẵn Sàng Phân Tích

Đường dẫn: `s3://lakehouse/gold/`

| Bảng | Mô tả | Số dòng |
|---|---|---|
| `feature_store` | Đặc trưng tổng hợp theo ngày/ô | 9,150 |
| `forest_health_index` | FHI tổng hợp theo ngày/ô | 9,150 |
| `plot_summary` | Tổng kết tình trạng mỗi ô | 25 |

**Cấu trúc `plot_summary`:**

```
plot_id, latest_date, latest_fhi, latest_status, ndvi,
avg_fhi, min_fhi, max_fhi,
total_disease_reports, total_drought_reports, total_anomalies
```

**Công thức Forest Health Index (FHI):**

FHI là điểm số từ 0–100 kết hợp nhiều chiều:

$$FHI = w_1 \cdot S_{ndvi} + w_2 \cdot S_{temp} + w_3 \cdot S_{moisture} + w_4 \cdot S_{disease} + w_5 \cdot S_{weather}$$

Phân loại:
- **Excellent** (≥ 80): Rừng rất khỏe mạnh
- **Good** (65–79): Rừng tốt
- **Fair** (50–64): Cần theo dõi
- **Poor** (35–49): Cần can thiệp
- **Critical** (< 35): Tình trạng nghiêm trọng

---

## 6. Thu Thập Dữ Liệu

### 6.1 Dữ Liệu Vệ Tinh Sentinel-2 (Thực Tế)

**Module**: `src/ingestion/satellite_ingest.py`

Hệ thống kết nối với **Microsoft Planetary Computer** — một nền tảng phân tích dữ liệu địa không gian miễn phí, sử dụng giao thức **STAC (SpatioTemporal Asset Catalog)** để truy vấn ảnh Sentinel-2 Level-2A.

```
STAC API: https://planetarycomputer.microsoft.com/api/stac/v1
Collection: sentinel-2-l2a
Band B08 (NIR) + Band B04 (Red) → NDVI = (B08 - B04) / (B08 + B04)
```

**Thông số lọc:**
- Mây che phủ tối đa: 30%
- Khoảng cách tối thiểu giữa 2 ảnh: 4 ngày
- Pixel sampling: Lấy trung bình từ cửa sổ 3×3 pixel quanh tọa độ mỗi ô

**Kết quả thực tế:**
- **575 bản ghi** NDVI thực (23 cảnh × 25 ô)
- **Phạm vi thời gian**: 2024-04-11 → 2025-03-27
- **Nguồn** (source): `sentinel2_l2a`
- **Giá trị NDVI**: 0.0625 – 0.6301

**Hai chế độ chạy:**
```bash
# Tải 1 năm dữ liệu lịch sử (overwrite)
python -m src.ingestion.satellite_ingest --mode historical

# Cập nhật 7 ngày gần nhất (append)
python -m src.ingestion.satellite_ingest --mode batch --days 7
```

**Fallback**: Nếu API không khả dụng, hệ thống tự động sinh dữ liệu tổng hợp theo mô hình mùa vụ Việt Nam.

### 6.2 Dữ Liệu Thời Tiết

**Module**: `src/ingestion/batch_ingest.py`, `src/generators/weather_fetcher.py`

Nguồn: **Open-Meteo API** (miễn phí, không cần API key)

```
URL: https://api.open-meteo.com/v1/forecast
Hourly: temperature_2m, relativehumidity_2m, precipitation
past_hours: 48 (batch update)
```

**Dữ liệu lịch sử**: API `https://archive-api.open-meteo.com/v1/archive` — 1 năm với 1 giờ/điểm.

### 6.3 Dữ Liệu Cảm Biến — Lịch Sử

**Module**: `src/generators/historical_data.py`

Sinh ~7.9 triệu bản ghi cảm biến lịch sử 1 năm với đặc trưng thực tế của Central Vietnam:

- **Nhiệt độ**: Cao nhất tháng 6–7 (~34°C), thấp nhất tháng 12–1 (~20°C), biến động ngày/đêm ±5°C
- **Độ ẩm**: Cao mùa mưa (tháng 9–12, 85–90%), thấp mùa khô (tháng 4–8, 65–70%)
- **Độ ẩm đất**: Theo sau lượng mưa với độ trễ, khô nhất tháng 4–5
- **Bất thường**: Đợt nhiệt (0.5%), khô hạn cục bộ (0.3%)
- **Ô bệnh**: 20% ô ngẫu nhiên bị bệnh, nhiệt độ +3°C, ẩm −10%

### 6.4 Ghi Chú Thực Địa & Ảnh (OCR)

**Module**: `src/generators/ocr_notes.py`, `src/generators/image_metadata.py`

Mô phỏng ghi chú tiếng Việt của cán bộ kiểm lâm với nội dung đa dạng:

```python
# Ví dụ ghi chú bệnh:
"Phát hiện đốm lá màu nâu, nghi ngờ bệnh nấm..."
"Cây có triệu chứng vàng lá, cần xử lý sớm..."

# Ví dụ ghi chú hạn hán:
"Đất khô nứt nẻ, cây sinh trưởng kém..."
"Thiếu nước nghiêm trọng, lá héo..."
```

Phân loại tự động theo từ khóa: `bệnh`, `nấm`, `vàng lá`, `sâu`, `hạn`, `khô`, `nứt`, v.v.

---

## 7. Luồng Streaming Thời Gian Thực

### Sơ Đồ Luồng

```
sensor_producer (container)
    │  generate_reading() mỗi 10 giây × 75 cảm biến
    │  JSON: {sensor_id, plot_id, timestamp, temperature, humidity, soil_moisture}
    ▼
kafka broker (kafka:29092)
    │  Topic: "sensor-readings"
    │  ~7-8 messages/second
    ▼
Spark Structured Streaming (stream_sensor.py)
    │  .readStream.format("kafka")
    │  Parse JSON → schema validation
    │  Thêm cột year_month
    ▼
Bronze Delta Table (s3://lakehouse/bronze/sensor_stream)
    │  mode: append
    │  checkpoint: s3://checkpoints/sensor_stream
    ▼
(Định kỳ) bronze_to_silver → silver_to_gold
```

### Module `sensor_producer`

File: `src/generators/sensor_producer.py`

- Kết nối Kafka với retry 30 lần, mỗi 5 giây
- Sinh dữ liệu theo mô hình khí hậu Central Vietnam (hàm `_seasonal_base`)
- Thêm nhiễu Gaussian cho tính thực tế
- Bất thường ngẫu nhiên: spike nhiệt độ (0.5%), drought drop (0.3%)
- Log mỗi 6 batch (1 phút)

---

## 8. Xử Lý & Làm Sạch Dữ Liệu (Silver Layer)

**Module**: `src/transform/bronze_to_silver.py`

### Quy Trình Xử Lý

```python
# Chạy bằng Spark (batch mode)
docker compose run --rm app python -m src.transform.bronze_to_silver
```

**Các bước transform:**

1. **Sensor**: Parse timestamp → clip outliers → tính is_anomaly flags → partition theo year_month
2. **Satellite**: Parse date → tính ndvi_30d_avg bằng rolling window → tính ndvi_change_pct → phân loại is_ndvi_anomaly
3. **Weather**: Deduplicate theo timestamp → kiểm tra phạm vi hợp lệ
4. **OCR**: Phát hiện keywords bệnh/hạn → thêm category, severity score
5. **Image**: Trích xuất metadata → tính health score từ labels

### Schema Silver — `satellite_clean`

```
plot_id         string   — ID ô giám sát
date            date     — Ngày chụp ảnh
ndvi            double   — Chỉ số thực vật (−1.0 đến 1.0)
latitude        double   — Vĩ độ
longitude       double   — Kinh độ
source          string   — "sentinel2_l2a" hoặc "synthetic"
cloud_cover     double   — Tỷ lệ mây (%)
year_month      string   — Partition key "YYYY-MM"
ndvi_30d_avg    double   — NDVI trung bình 30 ngày
ndvi_prev       double   — NDVI ngày trước
ndvi_change_pct double   — Thay đổi NDVI (%)
is_ndvi_anomaly boolean  — Bất thường NDVI
```

---

## 9. Tổng Hợp & Phân Tích (Gold Layer)

**Module**: `src/transform/silver_to_gold.py`

### `feature_store` — Kho Đặc Trưng

Tổng hợp tất cả nguồn dữ liệu theo ngày và ô, tạo vector đặc trưng cho ML và dashboard:

```
Key: (plot_id, date)
Features: avg_temp, avg_humidity, avg_soil_moisture,
          ndvi, ndvi_30d_avg, is_ndvi_anomaly,
          avg_precip, max_temp, min_humidity,
          disease_count, drought_count, anomaly_count
```

### `forest_health_index` — Chỉ Số Sức Khỏe

Tính FHI hàng ngày cho mỗi ô dựa trên feature_store:

- **FHI components**: NDVI score + temperature score + moisture score + disease penalty + weather bonus
- **Status**: Excellent / Good / Fair / Poor / Critical
- **Trend**: Increasing / Decreasing / Stable (so sánh 7 ngày)

### `plot_summary` — Tổng Kết 25 Ô

Bảng 25 dòng, 1 dòng/ô, chứa thông tin mới nhất và tổng kết toàn thời gian — được dùng trực tiếp bởi dashboard để tải nhanh mà không cần scan toàn bộ data.

---

## 10. Orchestration với Apache Airflow

Airflow 2.9.3 chạy với **LocalExecutor** trên PostgreSQL backend, lập lịch và theo dõi 2 DAGs chính.

### DAG 1: `forest_pipeline` — Pipeline Đầy Đủ

**File**: `dags/forest_pipeline_dag.py`

**Mục đích**: Khởi tạo toàn bộ hệ thống lần đầu (one-time setup)

**Luồng tasks:**
```
check_data_exists (BranchPythonOperator)
    │
    ├─► skip_pipeline   (nếu data đã tồn tại — idempotent)
    │
    └─► generate_historical_data
            │
            ├─► ingest_weather
            │       └─► bronze_to_silver
            │               └─► silver_to_gold
            │                       └─► validate_pipeline
            │
            └─► ingest_satellite
```

**Tính idempotent**: `src/check_data.py` kiểm tra sự tồn tại của Delta tables trước khi chạy — tránh chạy lại nếu đã có data.

### DAG 2: `forest_batch_update` — Cập Nhật Định Kỳ

**File**: `dags/forest_batch_dag.py`

**Schedule**: Mỗi 5 ngày (`timedelta(days=5)`)

**Luồng tasks:**
```
[batch_satellite_ingest, batch_weather_ingest]  ← song song
        │
        ├─► bronze_to_silver
                │
                └─► silver_to_gold
```

**Cập nhật satellite**: Gọi `python -m src.ingestion.satellite_ingest --mode batch --days 7`
**Cập nhật weather**: Gọi `python -m src.ingestion.batch_ingest`

### Giao Diện Airflow

Truy cập: http://localhost:8088 (admin/admin)

Các tính năng có thể sử dụng:
- Xem lịch chạy và trạng thái từng task
- Trigger DAG thủ công
- Xem Gantt chart thực thi
- Xem logs chi tiết từng task
- Quản lý connections và variables

---

## 11. Dashboard Trực Quan

**Công nghệ**: Streamlit, Plotly, Folium

**Truy cập**: http://localhost:8501

Dashboard gồm **4 trang** (pages):

### Trang 1: 🌲 Forest Overview (Tổng Quan)

**File**: `dashboard/pages/1_forest_overview.py`

- **Bản đồ tương tác** (Folium) hiển thị 25 ô với màu sắc theo FHI
- **Metrics tóm tắt**: Số ô theo từng trạng thái (Excellent/Good/Fair/Poor/Critical)
- **Biểu đồ phân phối FHI** theo histogram
- **Cảnh báo** các ô có tình trạng nghiêm trọng

### Trang 2: 📊 Health Dashboard (Sức Khỏe Chi Tiết)

**File**: `dashboard/pages/2_health_dashboard.py`

- **Bộ lọc**: Chọn ô (plot_id), chọn khoảng thời gian
- **Biều đồ time-series**: FHI theo thời gian cho ô được chọn
- **Cảm biến lazy loading**: Dùng Delta Lake filter để chỉ tải dữ liệu của ô được chọn (tránh OOM với 7.8M rows)
- **Biểu đồ subplot**: Nhiệt độ, độ ẩm, độ ẩm đất song song
- **NDVI trend** nếu có dữ liệu vệ tinh cho ô này

### Trang 3: 🌡️ Sensor Analysis (Phân Tích Cảm Biến)

**File**: `dashboard/pages/3_sensor_analysis.py`

- **Heatmap nhiệt độ** theo lưới 5×5 ô (snapshot gần nhất)
- **Phân phối thống kê** các chỉ số cảm biến
- **Phát hiện bất thường** — highlight các cảm biến có giá trị bất thường
- **So sánh cross-plot** giữa các ô

### Trang 4: 🛰️ Satellite View (Ảnh Vệ Tinh)

**File**: `dashboard/pages/4_satellite_view.py`

- **Tab 1 — Overview**: 
  - Grid 5×5 hiển thị NDVI mới nhất của từng ô (dùng `feature_df` nhẹ thay vì sensor data)
  - Bản đồ choropleth NDVI
  - Biểu đồ NDVI theo thời gian cho tất cả ô

- **Tab 2 — Plot Detail**:
  - Chọn ô → xem lịch sử NDVI của ô đó
  - Lazy load dữ liệu cảm biến của riêng ô đó (tránh OOM)
  - Subplot: NDVI + nhiệt độ + độ ẩm đất correlate với nhau

**Tối ưu bộ nhớ dashboard:**
- `plot_summary` (25 dòng) và `feature_store` (9,150 dòng) được cache với `@st.cache_data(ttl=300)`
- Dữ liệu cảm biến (7.8M dòng) chỉ được tải khi người dùng chọn ô cụ thể
- `mem_limit: 1g` trong Docker Compose (tăng từ 512MB)

---

## 12. Cấu Trúc Thư Mục

```
bigdata-final/
├── docker-compose.yml              # Định nghĩa 15 services
├── .env                            # Environment variables
├── run.sh                          # Script điều phối toàn bộ hệ thống
│
├── src/                            # Mã nguồn Python chính
│   ├── config.py                   # Cấu hình tập trung
│   ├── check_data.py               # Kiểm tra Delta table tồn tại
│   │
│   ├── generators/                 # Sinh và phát dữ liệu
│   │   ├── plots.py                # Định nghĩa 25 ô và 75 cảm biến
│   │   ├── historical_data.py      # Sinh 7.9M sensor rows lịch sử
│   │   ├── satellite_ndvi.py       # NDVI thực/tổng hợp
│   │   ├── weather_fetcher.py      # Lấy thời tiết từ Open-Meteo
│   │   ├── ocr_notes.py            # Sinh ghi chú thực địa tiếng Việt
│   │   ├── image_metadata.py       # Sinh metadata ảnh thực địa
│   │   └── sensor_producer.py      # Kafka producer real-time
│   │
│   ├── ingestion/                  # Thu thập dữ liệu ngoài
│   │   ├── satellite_ingest.py     # Sentinel-2 từ Planetary Computer
│   │   ├── batch_ingest.py         # Weather batch update
│   │   └── stream_sensor.py        # Kafka → Bronze streaming
│   │
│   ├── transform/                  # Xử lý dữ liệu
│   │   ├── bronze_to_silver.py     # Làm sạch & chuẩn hóa
│   │   └── silver_to_gold.py       # Tổng hợp & tính FHI
│   │
│   ├── validate/                   # Kiểm tra chất lượng
│   │   └── validate.py             # Data quality checks
│   │
│   └── utils/                      # Tiện ích
│       └── spark_utils.py          # SparkSession factory
│
├── dags/                           # Apache Airflow DAGs
│   ├── forest_pipeline_dag.py      # Pipeline đầy đủ (one-time)
│   └── forest_batch_dag.py         # Batch update (mỗi 5 ngày)
│
├── dashboard/                      # Streamlit dashboard
│   ├── app.py                      # Entry point
│   └── pages/
│       ├── 1_forest_overview.py    # Tổng quan rừng
│       ├── 2_health_dashboard.py   # Chi tiết sức khỏe
│       ├── 3_sensor_analysis.py    # Phân tích cảm biến
│       └── 4_satellite_view.py     # Góc nhìn vệ tinh
│
├── docker/                         # Dockerfiles
│   ├── app/
│   │   ├── Dockerfile              # Image cho app + sensor-producer
│   │   └── requirements.txt        # pandas, deltalake, kafka, pystac...
│   ├── spark/
│   │   ├── Dockerfile              # Spark + Delta Lake + MinIO support
│   │   └── requirements.txt
│   ├── dashboard/
│   │   ├── Dockerfile              # Streamlit + plotly + folium
│   │   └── requirements.txt
│   └── airflow/
│       ├── Dockerfile              # Airflow + tất cả dependencies
│       └── requirements.txt        # pystac-client, planetary-computer, rasterio
│
└── GIOI_THIEU.md                   # File tài liệu này
```

---

## 13. Hướng Dẫn Cài Đặt & Chạy

### Yêu Cầu Hệ Thống

| Thành phần | Yêu cầu tối thiểu |
|---|---|
| CPU | 4 cores |
| RAM | 8 GB (khuyến nghị 12 GB) |
| Disk | 20 GB trống |
| Docker | 24.0+ |
| Docker Compose | v2.20+ |
| Kết nối internet | Cần cho Planetary Computer + Open-Meteo |

### Chạy Lần Đầu

```bash
# 1. Clone và vào thư mục
cd /home/khiem2412/bigdata-final

# 2. Chạy toàn bộ hệ thống (script tự động)
./run.sh

# Script tự động thực hiện:
# - Kiểm tra Docker và Docker Compose
# - Build tất cả Docker images
# - Khởi động infrastructure (Kafka, MinIO, PostgreSQL)
# - Khởi tạo Airflow DB và user admin
# - Chờ services healthy
# - Chạy pipeline lần đầu (generate + ingest + transform)
# - Khởi động dashboard và Airflow
```

### Các Lệnh run.sh

```bash
./run.sh                 # Khởi động toàn bộ (default)
./run.sh --build         # Force rebuild images trước khi chạy
./run.sh --reset         # Xóa data cũ và chạy lại từ đầu
./run.sh status          # Xem trạng thái các services
./run.sh logs [service]  # Xem logs (tất cả hoặc service cụ thể)
./run.sh stop            # Dừng tất cả services
./run.sh down            # Dừng và xóa containers
```

### Chạy Từng Bước Thủ Công

```bash
# Khởi động infrastructure
docker compose up -d zookeeper kafka kafka-ui minio minio-init postgres

# Khởi động Spark cluster
docker compose up -d spark-master spark-worker

# Sinh dữ liệu lịch sử (lần đầu)
docker compose run --rm app python -m src.generators.historical_data

# Lấy dữ liệu vệ tinh thực (1 năm lịch sử)
docker compose run --rm app python -m src.ingestion.satellite_ingest --mode historical

# Lấy dữ liệu thời tiết
docker compose run --rm app python -m src.generators.weather_fetcher

# Sinh OCR notes và image metadata
docker compose run --rm app python -m src.generators.ocr_notes
docker compose run --rm app python -m src.generators.image_metadata

# Transform Bronze → Silver → Gold
docker compose run --rm app python -m src.transform.bronze_to_silver
docker compose run --rm app python -m src.transform.silver_to_gold

# Validate pipeline
docker compose run --rm app python -m src.validate.validate

# Khởi động streaming (real-time sensor)
docker compose up -d sensor-producer

# Khởi động dashboard
docker compose up -d dashboard

# Khởi động Airflow
docker compose up -d airflow-init
docker compose run --rm airflow-init  # Wait for init
docker compose up -d airflow-webserver airflow-scheduler
```

### Cập Nhật Dữ Liệu Định Kỳ

```bash
# Thủ công: cập nhật satellite + weather mới nhất
docker compose run --rm app python -m src.ingestion.satellite_ingest --mode batch --days 7
docker compose run --rm app python -m src.ingestion.batch_ingest

# Tự động: Airflow DAG "forest_batch_update" chạy mỗi 5 ngày
# Xem trạng thái tại: http://localhost:8088
```

### Dừng Hệ Thống

```bash
# Dừng nhưng giữ data
docker compose stop

# Dừng và xóa containers (giữ volumes/data)
docker compose down

# Dừng và xóa toàn bộ kể cả data (NGUY HIỂM!)
docker compose down -v
```

---

## 14. Cổng Truy Cập & Thông Tin Đăng Nhập

| Service | URL | Thông tin đăng nhập |
|---|---|---|
| **Dashboard Streamlit** | http://localhost:8501 | — |
| **Airflow Web UI** | http://localhost:8088 | admin / admin |
| **Kafka UI** | http://localhost:8080 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark Master UI** | http://localhost:8082 | — |
| **Spark Worker UI** | http://localhost:8081 | — |
| **Spark Job UI** | http://localhost:4040 | (chỉ khi Spark job đang chạy) |
| **Kafka Broker** | localhost:9092 | — |
| **MinIO S3 API** | http://localhost:9000 | minioadmin / minioadmin |

---

## 15. Chi Tiết Kỹ Thuật

### 15.1 Cấu Hình Delta Lake

Hệ thống sử dụng thư viện `deltalake` (Python) cho các batch operations và PySpark với Delta connector cho Spark jobs:

```python
DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_ENDPOINT_URL": "http://minio:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}
```

**Partition strategy**: Tất cả bảng Bronze và Silver đều partition theo `year_month` (dạng "YYYY-MM") để tối ưu quét dữ liệu theo thời gian.

### 15.2 Spark Configuration

```python
# SparkSession cho Delta + MinIO
spark = SparkSession.builder \
    .appName("ForestLakehouse") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,...") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

### 15.3 Kafka Configuration

```
Topic: sensor-readings
Partitions: 3 (default)
Replication: 1 (single broker)
Retention: 7 ngày (default)
Max message size: 1MB
```

**Producer settings**: `acks=1`, `retries=3`
**Consumer**: Spark Structured Streaming với `maxOffsetsPerTrigger=5000`

### 15.4 Planetary Computer STAC API

```python
import pystac_client
import planetary_computer

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=[lon_min, lat_min, lon_max, lat_max],
    datetime=f"{start_date}/{end_date}",
    query={"eo:cloud_cover": {"lt": 30}},
)
```

Tính NDVI:
```python
with item.assets["B08"].href as nir_file:  # Near-Infrared
    with item.assets["B04"].href as red_file:  # Red
        ndvi = (nir - red) / (nir + red + 1e-10)
```

### 15.5 Mô Hình Khí Hậu Central Vietnam

Hệ thống mô phỏng khí hậu Quảng Nam với các thông số thực tế:

```python
# Nhiệt độ (°C)
temp_seasonal = 27.0 + 7.0 * sin(2π × (doy - 80) / 365)  # ~20°C Jan, ~34°C Jul
temp_daily    = 5.0 * sin(2π × (hour - 6) / 24)            # Đỉnh 14h, đáy 6h

# Độ ẩm (%)
hum_seasonal  = 78.0 - 10.0 * sin(2π × (doy - 80) / 365)  # ~68% Jul, ~88% Jan

# Độ ẩm đất (m³/m³)
sm_seasonal   = 0.37 - 0.12 * sin(2π × (doy - 100) / 365)  # ~0.25 Jun, ~0.49 Dec
```

### 15.6 Tính Toán Tọa Độ Ô

Tọa độ địa lý chính xác của mỗi ô được tính từ tọa độ trung tâm:

```python
FOREST_CENTER_LAT = 15.3350  # °N
FOREST_CENTER_LON = 108.2528  # °E

DEG_PER_M_LAT = 1 / 111_320           # ≈ 8.983e-6 °/m
DEG_PER_M_LON = 1 / (111_320 × 0.9659)  # cos(15.335°) ≈ 9.299e-6 °/m

# Ô ở hàng r, cột c (1-indexed):
offset_x = (c - 1) × 20m - 50m + 10m   # Từ trái sang phải
offset_y = (r - 1) × 20m - 50m + 10m   # Từ dưới lên trên

lat = FOREST_CENTER_LAT + offset_y × DEG_PER_M_LAT
lon = FOREST_CENTER_LON + offset_x × DEG_PER_M_LON
```

### 15.7 Libraries & Dependencies Chính

**docker/app/requirements.txt** (Python 3.11):
```
pandas>=2.0
deltalake>=0.17
kafka-python>=2.0
pystac-client>=0.7.0
planetary-computer>=1.0.0
rasterio>=1.3.0
pyarrow>=14.0
requests>=2.31
numpy>=1.24
```

**docker/spark/requirements.txt**:
```
pyspark>=3.5
delta-spark>=3.1
hadoop-aws (via JAR)
```

**docker/dashboard/requirements.txt**:
```
streamlit>=1.28
plotly>=5.18
folium>=0.15
streamlit-folium>=0.15
pandas>=2.0
deltalake>=0.17
pyarrow>=14.0
```

**docker/airflow/requirements.txt** (Airflow 2.9.3):
```
apache-airflow>=2.9.3
pystac-client>=0.7.0
planetary-computer>=1.0.0
rasterio>=1.3.0
pandas>=2.0
deltalake>=0.17
```

### 15.8 Lưu Trữ Persistent

Hai Docker volumes đảm bảo dữ liệu không bị mất khi restart:

| Volume | Nội dung | Mount point |
|---|---|---|
| `minio-data` | Toàn bộ Delta Lake tables (Bronze/Silver/Gold) | `/data` trong MinIO container |
| `postgres-data` | Airflow metadata (DAG runs, logs) | `/var/lib/postgresql/data` |
| `airflow-logs` | Task execution logs | `/opt/airflow/logs` |

### 15.9 Bảo Mật Mạng

Tất cả containers giao tiếp qua mạng Docker nội bộ `lakehouse` (bridge driver). Chỉ các cổng cần thiết mới được expose ra host:

- Kafka: `9092` (external), `29092` (internal chỉ container-to-container)
- ZooKeeper: không expose ra ngoài
- PostgreSQL: không expose ra ngoài
- MinIO: `9000` (API), `9001` (Console)

---

## Tóm Tắt Nhanh

| Mục | Thông tin |
|---|---|
| Tổng services | 15 Docker containers |
| Tổng cảm biến | 75 (25 ô × 3 cảm biến/ô) |
| Dữ liệu lịch sử | 7,865,163 sensor rows |
| Dữ liệu vệ tinh | 575 records (Sentinel-2 L2A thực) |
| FHI records | 9,150 (25 ô × 366 ngày) |
| Stack chính | Kafka + Spark + Delta Lake + MinIO + Airflow + Streamlit |
| Vị trí | 15.3350°N, 108.2528°E (Quảng Nam, VN) |
| Cập nhật tự động | Mỗi 5 ngày (satellite + weather) + real-time streaming |

---

*Tài liệu này được tạo tự động từ mã nguồn hệ thống. Cập nhật lần cuối: 2025.*
