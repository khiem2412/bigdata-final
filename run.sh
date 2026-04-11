#!/usr/bin/env bash
# ==============================================================================
# Forest Health Monitoring Lakehouse - Demo Orchestration Script
# ==============================================================================
# Usage: ./run.sh <command>
#
# Infrastructure:
#   start       - Start all services (infra + Spark cluster + Airflow + dashboard)
#   stop        - Stop all services
#   status      - Show container status & UI links
#   health      - Health check all services
#   logs <svc>  - Follow logs for a service
#
# Pipeline:
#   generate    - Generate 1 year historical data
#   pipeline    - Trigger full pipeline via Airflow DAG
#   transform   - Run Bronze → Silver → Gold (Spark cluster)
#   stream      - Start real-time streaming (Kafka → Spark)
#   stop-stream - Stop streaming pipeline
#   validate    - Validate all data layers
#
# Demo:
#   demo        - Full demo: start → generate → transform → validate → ready!
#   setup       - Build Docker images only
#   clean       - Remove all containers, volumes, data
# ==============================================================================

set -euo pipefail

# ──── Colors ────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ──── Project root ────
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# ──── Logging ────
info()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn()   { echo -e "${YELLOW}[!]${NC} $*"; }
error()  { echo -e "${RED}[✗]${NC} $*"; }
step()   { echo -e "${CYAN}[$1]${NC} $2"; }
header() {
    echo ""
    echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  ${BOLD}$*${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
}

banner() {
    echo -e "${GREEN}"
    echo "  ╔═══════════════════════════════════════════════════╗"
    echo "  ║     🌲 Forest Health Monitoring Lakehouse 🌲     ║"
    echo "  ║                                                   ║"
    echo "  ║  PySpark · Delta Lake · Kafka · Airflow · MinIO   ║"
    echo "  ╚═══════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# ──── Helpers ────
wait_for_healthy() {
    local container=$1
    local timeout=${2:-60}
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        local health
        health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "not_found")
        if [ "$health" = "healthy" ]; then
            info "$container is healthy"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    error "$container did not become healthy within ${timeout}s"
    return 1
}

wait_for_url() {
    local url=$1
    local name=$2
    local timeout=${3:-60}
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            info "$name is ready"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    warn "$name not responding at $url (may need more time)"
    return 1
}

ensure_infra() {
    if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q "forest-kafka"; then
        warn "Infrastructure not running. Starting..."
        cmd_start
    fi
}

# Check if data exists in a given layer (bronze/silver/gold)
# Returns 0 if data exists, 1 if not
data_exists() {
    local layer=${1:-"bronze"}
    docker compose run --rm -T app python -m src.check_data --layer "$layer" > /dev/null 2>&1
    return $?
}

print_urls() {
    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║              📡 SERVICE UI ACCESS POINTS                ║${NC}"
    echo -e "${BOLD}╠══════════════════════════════════════════════════════════╣${NC}"
    echo -e "${BOLD}║${NC}  ${CYAN}Airflow${NC}       http://localhost:${BOLD}8088${NC}  (admin / admin)     ${BOLD}║${NC}"
    echo -e "${BOLD}║${NC}  ${CYAN}Dashboard${NC}     http://localhost:${BOLD}8501${NC}                      ${BOLD}║${NC}"
    echo -e "${BOLD}║${NC}  ${CYAN}Spark Master${NC}  http://localhost:${BOLD}8082${NC}                      ${BOLD}║${NC}"
    echo -e "${BOLD}║${NC}  ${CYAN}Spark Worker${NC}  http://localhost:${BOLD}8081${NC}                      ${BOLD}║${NC}"
    echo -e "${BOLD}║${NC}  ${CYAN}MinIO Console${NC} http://localhost:${BOLD}9001${NC}  (minioadmin)        ${BOLD}║${NC}"
    echo -e "${BOLD}║${NC}  ${CYAN}Kafka UI${NC}      http://localhost:${BOLD}8080${NC}                      ${BOLD}║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# ══════════════════════════════════════════════════════════════════════
# COMMANDS
# ══════════════════════════════════════════════════════════════════════

cmd_setup() {
    header "SETUP - Building Docker Images"

    step "1/3" "Building Spark image (master/worker/jobs)..."
    docker compose build spark-master

    step "2/3" "Building Airflow image..."
    docker compose build airflow-scheduler

    step "3/3" "Building app & dashboard images..."
    docker compose build app dashboard

    info "All images built successfully"
}

cmd_start() {
    header "STARTING ALL SERVICES"

    step "1/6" "Starting infrastructure (Zookeeper, Kafka, MinIO, PostgreSQL)..."
    docker compose up -d zookeeper kafka minio postgres

    step "2/6" "Waiting for infrastructure to be healthy..."
    wait_for_healthy forest-zookeeper 60
    wait_for_healthy forest-kafka 90
    wait_for_healthy forest-minio 60
    wait_for_healthy forest-postgres 30

    step "3/6" "Initializing MinIO buckets..."
    docker compose up minio-init

    step "4/6" "Starting Spark cluster (master + worker)..."
    docker compose up -d spark-master spark-worker kafka-ui
    sleep 5
    info "Spark Master UI: http://localhost:8082"

    step "5/6" "Starting Airflow (init → webserver → scheduler)..."
    docker compose up airflow-init
    docker compose up -d airflow-webserver airflow-scheduler
    wait_for_url "http://localhost:8088/health" "Airflow" 90

    step "6/6" "Starting Dashboard..."
    docker compose up -d dashboard sensor-producer
    sleep 3

    info "All services started successfully"
    print_urls
}

cmd_health() {
    header "HEALTH CHECK"

    local all_ok=true

    for svc in "forest-zookeeper:Zookeeper" "forest-kafka:Kafka" "forest-minio:MinIO" "forest-postgres:PostgreSQL"; do
        local container="${svc%%:*}"
        local name="${svc##*:}"
        if docker compose ps "$container" 2>/dev/null | grep -q "running\|Up"; then
            local health
            health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "n/a")
            if [ "$health" = "healthy" ]; then
                info "$name: ✓ Healthy"
            else
                warn "$name: Running (health: $health)"
            fi
        else
            error "$name: ✗ Not running"
            all_ok=false
        fi
    done

    # Spark cluster
    if docker ps --format '{{.Names}}' | grep -q "forest-spark-master"; then
        info "Spark Master: ✓ Running (http://localhost:8082)"
    else
        warn "Spark Master: - Not started"
    fi

    if docker ps --format '{{.Names}}' | grep -q "forest-spark-worker"; then
        info "Spark Worker: ✓ Running (http://localhost:8081)"
    else
        warn "Spark Worker: - Not started"
    fi

    # Airflow
    if docker ps --format '{{.Names}}' | grep -q "forest-airflow-webserver"; then
        info "Airflow:      ✓ Running (http://localhost:8088)"
    else
        warn "Airflow:      - Not started"
    fi

    # Dashboard
    if docker ps --format '{{.Names}}' | grep -q "forest-dashboard"; then
        info "Dashboard:    ✓ Running (http://localhost:8501)"
    else
        warn "Dashboard:    - Not started"
    fi

    # Streaming
    if docker ps --format '{{.Names}}' | grep -q "forest-sensor-producer"; then
        info "Streaming:    ✓ Active"
    else
        info "Streaming:    - Idle"
    fi

    if $all_ok; then
        echo ""
        info "All core services healthy ✓"
    fi
}

cmd_generate() {
    local force=false
    if [[ "${1:-}" == "--force" ]]; then
        force=true
    fi

    header "GENERATE - Historical Data (1 Year)"

    ensure_infra

    if ! $force && data_exists bronze; then
        info "Bronze data already exists. Skipping generation."
        info "Use './run.sh generate --force' to regenerate."
        return 0
    fi

    if $force; then
        warn "Force mode: regenerating all data..."
    fi

    step "1/5" "Generating historical sensor data (~7.9M rows)..."
    docker compose run --rm app python -m src.generators.historical_data

    step "2/5" "Fetching weather data (Open-Meteo API)..."
    docker compose run --rm app python -m src.generators.weather_fetcher

    step "3/5" "Generating NDVI satellite data..."
    docker compose run --rm app python -m src.generators.satellite_ndvi

    step "4/5" "Generating OCR field notes..."
    docker compose run --rm app python -m src.generators.ocr_notes

    step "5/5" "Generating image metadata..."
    docker compose run --rm app python -m src.generators.image_metadata

    info "Historical data generation complete ✓"
}

cmd_transform() {
    local force=false
    if [[ "${1:-}" == "--force" ]]; then
        force=true
    fi

    header "TRANSFORM - Bronze → Silver → Gold (Spark Cluster)"

    ensure_infra

    if ! $force && data_exists gold; then
        info "Gold data already exists. Skipping transformation."
        info "Use './run.sh transform --force' to re-transform."
        return 0
    fi

    step "1/2" "Bronze → Silver transformation..."
    docker compose run --rm spark python -m src.transform.bronze_to_silver

    step "2/2" "Silver → Gold transformation..."
    docker compose run --rm spark python -m src.transform.silver_to_gold

    info "All transformations complete ✓"
}

cmd_pipeline() {
    header "PIPELINE - Trigger Airflow DAG"

    if ! docker ps --format '{{.Names}}' | grep -q "forest-airflow-scheduler"; then
        error "Airflow scheduler not running. Start services first: ./run.sh start"
        exit 1
    fi

    step "1/2" "Unpausing DAG..."
    docker compose exec -T airflow-scheduler airflow dags unpause forest_lakehouse_pipeline

    step "2/2" "Triggering pipeline DAG..."
    docker compose exec -T airflow-scheduler airflow dags trigger forest_lakehouse_pipeline

    info "Pipeline DAG triggered! Monitor at: http://localhost:8088"
    info "DAG: forest_lakehouse_pipeline"
}

cmd_stream() {
    header "STREAM - Real-time Sensor Pipeline"

    ensure_infra

    step "1/2" "Starting sensor producer (Kafka)..."
    docker compose up -d sensor-producer

    step "2/2" "Starting Spark Structured Streaming consumer..."
    docker compose run -d --name forest-stream-consumer spark python -m src.ingestion.stream_sensor

    info "Streaming pipeline started"
    info "  Producer → Kafka → Spark → Bronze Delta"
    info "  Stop with: ./run.sh stop-stream"
}

cmd_stop_stream() {
    info "Stopping streaming pipeline..."
    docker compose stop sensor-producer 2>/dev/null || true
    docker stop forest-stream-consumer 2>/dev/null || true
    docker rm forest-stream-consumer 2>/dev/null || true
    info "Streaming pipeline stopped"
}

cmd_validate() {
    header "VALIDATE - Data Quality Check"
    ensure_infra
    docker compose run --rm app python -m src.validate
}

cmd_demo() {
    local start_time=$SECONDS
    banner
    header "FULL DEMO PIPELINE"

    step "1/5" "Building Docker images..."
    cmd_setup

    step "2/5" "Starting all services..."
    cmd_start

    step "3/5" "Generating historical data (skips if exists)..."
    cmd_generate

    step "4/5" "Running transformations (skips if exists)..."
    cmd_transform

    step "5/5" "Validating data..."
    cmd_validate

    local elapsed=$((SECONDS - start_time))
    local minutes=$((elapsed / 60))
    local seconds=$((elapsed % 60))

    echo ""
    header "🎉 DEMO READY - ${minutes}m ${seconds}s"
    print_urls
    echo -e "  ${BOLD}Next steps:${NC}"
    echo "    ./run.sh stream      # Start real-time streaming"
    echo "    ./run.sh pipeline    # Trigger pipeline via Airflow"
    echo ""
}

cmd_stop() {
    header "STOPPING ALL SERVICES"

    cmd_stop_stream 2>/dev/null || true
    docker compose down

    info "All services stopped"
}

cmd_clean() {
    header "CLEANUP"

    read -p "⚠ This will delete ALL containers, volumes, and data. Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        info "Cancelled"
        return 0
    fi

    cmd_stop_stream 2>/dev/null || true
    docker compose down -v --remove-orphans
    docker image prune -f --filter "label=com.docker.compose.project=bigdata-final" 2>/dev/null || true

    info "Cleanup complete"
}

cmd_logs() {
    local service=${1:-""}
    if [ -z "$service" ]; then
        echo "Usage: ./run.sh logs <service>"
        echo ""
        echo "Services:"
        echo "  zookeeper kafka minio kafka-ui postgres"
        echo "  spark-master spark-worker"
        echo "  airflow-webserver airflow-scheduler"
        echo "  app sensor-producer dashboard"
        return 1
    fi
    docker compose logs -f --tail=100 "$service"
}

cmd_status() {
    header "CONTAINER STATUS"
    docker compose ps -a
    print_urls
}

# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

show_help() {
    banner
    echo "Usage: ./run.sh <command>"
    echo ""
    echo -e "${BOLD}Infrastructure:${NC}"
    echo "  start         Start all services (infra + Spark + Airflow + dashboard)"
    echo "  stop          Stop all services"
    echo "  status        Show container status & UI links"
    echo "  health        Health check all services"
    echo "  logs <svc>    Follow logs for a service"
    echo ""
    echo -e "${BOLD}Pipeline:${NC}"
    echo "  generate      Generate 1 year historical data (skips if exists)"
    echo "  generate --force  Force regenerate all data"
    echo "  pipeline      Trigger full pipeline via Airflow"
    echo "  transform     Run Bronze → Silver → Gold (skips if exists)"
    echo "  transform --force  Force re-transform all data"
    echo "  stream        Start real-time streaming"
    echo "  stop-stream   Stop streaming pipeline"
    echo "  validate      Validate all data layers"
    echo ""
    echo -e "${BOLD}Demo:${NC}"
    echo "  demo          Full demo (setup → start → generate → transform → validate)"
    echo "  setup         Build Docker images only"
    echo "  clean         Remove all containers, volumes, data"
    echo ""
}

main() {
    local cmd=${1:-help}
    shift || true

    case $cmd in
        setup)        cmd_setup "$@" ;;
        start)        cmd_start "$@" ;;
        stop)         cmd_stop "$@" ;;
        health)       cmd_health "$@" ;;
        generate)     cmd_generate "$@" ;;
        transform)    cmd_transform "$@" ;;
        pipeline)     cmd_pipeline "$@" ;;
        stream)       cmd_stream "$@" ;;
        stop-stream)  cmd_stop_stream "$@" ;;
        validate)     cmd_validate "$@" ;;
        demo)         cmd_demo "$@" ;;
        clean)        cmd_clean "$@" ;;
        logs)         cmd_logs "$@" ;;
        status)       cmd_status "$@" ;;
        help|--help|-h)  show_help ;;
        *)
            error "Unknown command: $cmd"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
