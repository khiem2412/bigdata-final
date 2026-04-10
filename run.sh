#!/usr/bin/env bash
# ==============================================================================
# Forest Lakehouse - Unified Orchestration Script
# ==============================================================================
# Usage: ./run.sh <command>
#
# Commands:
#   setup       - Build Docker images
#   start       - Start infrastructure services (Kafka, MinIO, Zookeeper)
#   health      - Check health of all services
#   generate    - Generate 1 year historical data (sensor, weather, NDVI, OCR, images)
#   transform   - Run Bronze → Silver → Gold transformations
#   stream      - Start real-time sensor streaming (Kafka producer + Spark consumer)
#   batch       - Run batch weather ingestion
#   dashboard   - Start Streamlit dashboard
#   validate    - Validate all data in the lakehouse
#   demo        - Full demo pipeline (setup → start → generate → transform → dashboard)
#   stop        - Stop all services
#   clean       - Clean up everything (containers, volumes, data)
#   logs        - Show logs for a service
#   status      - Show status of all containers
# ==============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Project root
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# Logging
log_info()  { echo -e "${GREEN}[INFO]${NC}  $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC}  $(date '+%H:%M:%S') $*"; }
log_header() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $*${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════${NC}"
}

# Retry wrapper
retry() {
    local max_attempts=$1
    local delay=$2
    shift 2
    local cmd="$*"
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if eval "$cmd"; then
            return 0
        fi
        log_warn "Attempt $attempt/$max_attempts failed. Retrying in ${delay}s..."
        sleep "$delay"
        attempt=$((attempt + 1))
    done
    log_error "Command failed after $max_attempts attempts: $cmd"
    return 1
}

# ==============================================================================
# COMMANDS
# ==============================================================================

cmd_setup() {
    log_header "SETUP - Building Docker Images"

    log_step "Building app image..."
    docker compose build app

    log_step "Building spark image..."
    docker compose build spark

    log_step "Building dashboard image..."
    docker compose build dashboard

    log_info "All images built successfully"
}

cmd_start() {
    log_header "START - Infrastructure Services"

    log_step "Starting Zookeeper, Kafka, MinIO..."
    docker compose up -d zookeeper kafka minio

    log_step "Waiting for services to be healthy..."
    wait_for_healthy "forest-zookeeper" 60
    wait_for_healthy "forest-kafka" 90
    wait_for_healthy "forest-minio" 60

    log_step "Initializing MinIO buckets..."
    docker compose up minio-init

    log_info "Infrastructure services are ready"
    cmd_health
}

cmd_health() {
    log_header "HEALTH CHECK"

    local all_ok=true

    # Check Zookeeper
    if docker compose ps zookeeper | grep -q "running"; then
        log_info "Zookeeper: ✓ Running"
    else
        log_error "Zookeeper: ✗ Not running"
        all_ok=false
    fi

    # Check Kafka
    if docker compose ps kafka | grep -q "running"; then
        log_info "Kafka: ✓ Running"
        # Check Kafka broker
        if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; then
            log_info "  Kafka broker: ✓ Responsive"
        else
            log_warn "  Kafka broker: ⚠ Not responsive yet"
        fi
    else
        log_error "Kafka: ✗ Not running"
        all_ok=false
    fi

    # Check MinIO
    if docker compose ps minio | grep -q "running"; then
        log_info "MinIO: ✓ Running"
        if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            log_info "  MinIO API: ✓ Healthy (http://localhost:9000)"
            log_info "  MinIO Console: http://localhost:9001"
        else
            log_warn "  MinIO API: ⚠ Not responding"
        fi
    else
        log_error "MinIO: ✗ Not running"
        all_ok=false
    fi

    # Check Dashboard
    if docker compose ps dashboard 2>/dev/null | grep -q "running"; then
        log_info "Dashboard: ✓ Running (http://localhost:8501)"
    else
        log_info "Dashboard: - Not started"
    fi

    # Check streaming
    if docker compose ps sensor-producer 2>/dev/null | grep -q "running"; then
        log_info "Sensor Producer: ✓ Streaming"
    else
        log_info "Sensor Producer: - Not started"
    fi

    if $all_ok; then
        log_info "All infrastructure services healthy ✓"
    else
        log_error "Some services are unhealthy"
        return 1
    fi
}

cmd_generate() {
    log_header "GENERATE - Historical Data (1 Year)"

    ensure_infra

    log_step "1/5 Generating historical sensor data (~7.9M rows)..."
    docker compose run --rm app python -m src.generators.historical_data

    log_step "2/5 Fetching weather data (Open-Meteo API)..."
    docker compose run --rm app python -m src.generators.weather_fetcher

    log_step "3/5 Generating NDVI satellite data..."
    docker compose run --rm app python -m src.generators.satellite_ndvi

    log_step "4/5 Generating OCR field notes..."
    docker compose run --rm app python -m src.generators.ocr_notes

    log_step "5/5 Generating image metadata..."
    docker compose run --rm app python -m src.generators.image_metadata

    log_info "Historical data generation complete ✓"
}

cmd_transform() {
    log_header "TRANSFORM - Bronze → Silver → Gold"

    ensure_infra

    log_step "1/2 Bronze → Silver transformation..."
    docker compose run --rm spark python -m src.transform.bronze_to_silver

    log_step "2/2 Silver → Gold transformation..."
    docker compose run --rm spark python -m src.transform.silver_to_gold

    log_info "All transformations complete ✓"
}

cmd_stream() {
    log_header "STREAM - Real-time Sensor Pipeline"

    ensure_infra

    log_step "Starting sensor producer (Kafka)..."
    docker compose up -d sensor-producer

    log_step "Starting Spark Structured Streaming consumer..."
    docker compose run -d --name forest-stream-consumer spark python -m src.ingestion.stream_sensor

    log_info "Streaming pipeline started"
    log_info "  Producer → Kafka → Spark → Bronze Delta"
    log_info "  Stop with: ./run.sh stop-stream"
}

cmd_stop_stream() {
    log_info "Stopping streaming pipeline..."
    docker compose stop sensor-producer 2>/dev/null || true
    docker stop forest-stream-consumer 2>/dev/null || true
    docker rm forest-stream-consumer 2>/dev/null || true
    log_info "Streaming pipeline stopped"
}

cmd_batch() {
    log_header "BATCH - Weather Ingestion"

    ensure_infra

    log_step "Running weather batch ingestion..."
    docker compose run --rm app python -m src.ingestion.batch_ingest

    log_info "Batch ingestion complete ✓"
}

cmd_dashboard() {
    log_header "DASHBOARD - Starting Streamlit"

    ensure_infra

    log_step "Starting dashboard..."
    docker compose up -d dashboard

    log_info "Dashboard started at http://localhost:8501"
}

cmd_validate() {
    log_header "VALIDATE - Data Quality Check"

    ensure_infra

    docker compose run --rm app python -m src.validate
}

cmd_demo() {
    log_header "FULL DEMO PIPELINE"
    local start_time=$SECONDS

    cmd_setup
    cmd_start
    cmd_generate
    cmd_transform
    cmd_validate
    cmd_dashboard

    local elapsed=$((SECONDS - start_time))
    local minutes=$((elapsed / 60))
    local seconds=$((elapsed % 60))

    log_header "DEMO READY"
    log_info "Total time: ${minutes}m ${seconds}s"
    log_info ""
    log_info "Access points:"
    log_info "  Dashboard:    http://localhost:8501"
    log_info "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    log_info ""
    log_info "Optional - Start streaming:"
    log_info "  ./run.sh stream"
}

cmd_stop() {
    log_header "STOP - All Services"

    cmd_stop_stream 2>/dev/null || true
    docker compose down

    log_info "All services stopped"
}

cmd_clean() {
    log_header "CLEAN - Full Cleanup"

    read -p "This will delete all containers, volumes, and data. Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Cleanup cancelled"
        return 0
    fi

    cmd_stop_stream 2>/dev/null || true
    docker compose down -v --remove-orphans

    # Remove dangling images
    docker image prune -f --filter "label=com.docker.compose.project=bigdata-final" 2>/dev/null || true

    log_info "Cleanup complete"
}

cmd_logs() {
    local service=${1:-""}
    if [ -z "$service" ]; then
        echo "Usage: ./run.sh logs <service>"
        echo "Services: zookeeper, kafka, minio, spark, app, sensor-producer, dashboard"
        return 1
    fi
    docker compose logs -f --tail=100 "$service"
}

cmd_status() {
    log_header "CONTAINER STATUS"
    docker compose ps -a
}

# ==============================================================================
# HELPERS
# ==============================================================================

wait_for_healthy() {
    local container=$1
    local timeout=${2:-60}
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        local health
        health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "not_found")

        case $health in
            healthy)
                log_info "$container is healthy"
                return 0
                ;;
            not_found)
                log_warn "$container not found, waiting..."
                ;;
            *)
                # Still starting
                ;;
        esac

        sleep 3
        elapsed=$((elapsed + 3))
    done

    log_error "$container did not become healthy within ${timeout}s"
    return 1
}

ensure_infra() {
    if ! docker compose ps kafka 2>/dev/null | grep -q "running"; then
        log_warn "Infrastructure not running. Starting..."
        cmd_start
    fi
}

# ==============================================================================
# MAIN
# ==============================================================================

show_help() {
    echo ""
    echo -e "${CYAN}Forest Lakehouse - Orchestration Script${NC}"
    echo ""
    echo "Usage: ./run.sh <command>"
    echo ""
    echo "Commands:"
    echo "  setup         Build Docker images"
    echo "  start         Start infrastructure (Kafka, MinIO, Zookeeper)"
    echo "  health        Check health of all services"
    echo "  generate      Generate 1 year historical data"
    echo "  transform     Run Bronze → Silver → Gold transformations"
    echo "  stream        Start real-time streaming pipeline"
    echo "  stop-stream   Stop streaming pipeline"
    echo "  batch         Run batch weather ingestion"
    echo "  dashboard     Start Streamlit dashboard"
    echo "  validate      Validate all data"
    echo "  demo          Full pipeline (setup → generate → transform → dashboard)"
    echo "  stop          Stop all services"
    echo "  clean         Remove all containers, volumes, and data"
    echo "  logs <svc>    Show logs for a service"
    echo "  status        Show container status"
    echo ""
}

main() {
    local cmd=${1:-help}
    shift || true

    case $cmd in
        setup)        cmd_setup "$@" ;;
        start)        cmd_start "$@" ;;
        health)       cmd_health "$@" ;;
        generate)     cmd_generate "$@" ;;
        transform)    cmd_transform "$@" ;;
        stream)       cmd_stream "$@" ;;
        stop-stream)  cmd_stop_stream "$@" ;;
        batch)        cmd_batch "$@" ;;
        dashboard)    cmd_dashboard "$@" ;;
        validate)     cmd_validate "$@" ;;
        demo)         cmd_demo "$@" ;;
        stop)         cmd_stop "$@" ;;
        clean)        cmd_clean "$@" ;;
        logs)         cmd_logs "$@" ;;
        status)       cmd_status "$@" ;;
        help|--help|-h)  show_help ;;
        *)
            log_error "Unknown command: $cmd"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
