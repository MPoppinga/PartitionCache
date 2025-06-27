#!/bin/bash
set -e

echo "🚀 Setting up complete integration testing environment"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="docker-compose.integration.yml"
DEFAULT_SERVICES="postgres-integration redis-integration"
ALL_SERVICES="postgres-integration redis-integration mysql-integration"

# Function to show help
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Set up integration testing environment for PartitionCache"
    echo ""
    echo "Options:"
    echo "  --all-services       Start all services (PostgreSQL, Redis, MySQL)"
    echo "  --service SERVICE    Start specific service(s) (comma-separated)"
    echo "  --list-services      List available services"
    echo "  --no-wait           Don't wait for services to be healthy"
    echo "  --help, -h          Show this help message"
    echo ""
    echo "Services:"
    echo "  postgres-integration    PostgreSQL 15 with extensions"
    echo "  redis-integration      Redis 7 with persistence"
    echo "  mysql-integration      MySQL 8.0"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Start PostgreSQL and Redis"
    echo "  $0 --all-services                    # Start all services"
    echo "  $0 --service postgres-integration    # Start only PostgreSQL"
    echo "  $0 --service postgres-integration,redis-integration  # Start specific services"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo -e "${RED}❌ Docker is not running. Please start Docker first.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Docker is running${NC}"
}

# Function to check if docker-compose is available
check_docker_compose() {
    if command -v docker-compose >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker compose"
    else
        echo -e "${RED}❌ Neither docker-compose nor 'docker compose' is available${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Using: $DOCKER_COMPOSE${NC}"
}

# Function to list available services
list_services() {
    echo -e "${BLUE}📋 Available services:${NC}"
    echo "  postgres-integration  - PostgreSQL 15 with pg_cron and roaringbitmap"
    echo "  redis-integration     - Redis 7 Alpine with persistence"
    echo "  mysql-integration     - MySQL 8.0 with optimized configuration"
    echo ""
    echo -e "${YELLOW}Default services: $DEFAULT_SERVICES${NC}"
    echo -e "${YELLOW}All services: $ALL_SERVICES${NC}"
    exit 0
}

# Function to start services
start_services() {
    local services=$1
    echo -e "${YELLOW}🐳 Starting integration services: $services${NC}"
    
    cd "$(dirname "$0")/.."
    
    # Build postgres image if it doesn't exist
    if [[ -z "$(docker images -q partitioncache-postgres:latest 2> /dev/null)" ]]; then
        echo -e "${YELLOW}🛠️ Building partitioncache-postgres image...${NC}"
        docker build -t partitioncache-postgres:latest ./.github/docker/postgres-cron
    fi

    # Pull images first
    echo -e "${YELLOW}📥 Pulling container images...${NC}"
    $DOCKER_COMPOSE -f $COMPOSE_FILE pull $services
    
    # Start services
    $DOCKER_COMPOSE -f $COMPOSE_FILE up -d $services
    
    if [ "$WAIT_FOR_HEALTH" = true ]; then
        wait_for_services "$services"
    fi
}

# Function to wait for services to be healthy
wait_for_services() {
    local services=$1
    echo -e "${YELLOW}⏳ Waiting for services to be healthy...${NC}"
    
    local timeout=180
    local counter=0
    local healthy=false
    
    while [ $counter -lt $timeout ] && [ "$healthy" = false ]; do
        healthy=true
        
        for service in $services; do
            local container_id=$($DOCKER_COMPOSE -f $COMPOSE_FILE ps -q $service)
            if [ -z "$container_id" ]; then
                healthy=false
                break
            fi
            local health_status=$(docker inspect --format '{{.State.Health.Status}}' $container_id)
            
            if [ "$health_status" != "healthy" ]; then
                healthy=false
                break
            fi
        done
        
        if [ "$healthy" = true ]; then
            echo -e "${GREEN}✅ All services are healthy!${NC}"
            break
        fi
        
        sleep 2
        counter=$((counter + 2))
        
        # Show progress every 20 seconds
        if [ $((counter % 20)) -eq 0 ]; then
            echo -e "${YELLOW}⏳ Still waiting... ($counter/${timeout}s)${NC}"
        fi
    done
    
    if [ "$healthy" = false ]; then
        echo -e "${RED}❌ Services failed to become healthy within $timeout seconds${NC}"
        show_service_status
        exit 1
    fi
}

# Function to show service status
show_service_status() {
    echo -e "${BLUE}📊 Service Status:${NC}"
    $DOCKER_COMPOSE -f $COMPOSE_FILE ps
}

# Function to create environment configuration
create_env_config() {
    local services=$1
    echo -e "${YELLOW}📝 Creating environment configuration...${NC}"
    
    cat > .env.integration << EOF
# Integration Testing Environment Configuration
# Generated by setup_integration_testing.sh
# Uses standard KEY=value format (no export statements)

# PostgreSQL Configuration
PG_HOST=localhost
PG_PORT=5434
PG_USER=integration_user
PG_PASSWORD=integration_password
PG_DBNAME=partitioncache_integration

# Queue Configuration (uses same PostgreSQL)
PG_QUEUE_HOST=localhost
PG_QUEUE_PORT=5434
PG_QUEUE_USER=integration_user
PG_QUEUE_PASSWORD=integration_password
PG_QUEUE_DB=partitioncache_integration
QUERY_QUEUE_PROVIDER=postgresql

# PostgreSQL Cache Backend Configuration
DB_HOST=localhost
DB_PORT=5434
DB_USER=integration_user
DB_PASSWORD=integration_password
DB_NAME=partitioncache_integration

# PostgreSQL Array Cache Configuration  
PG_ARRAY_CACHE_TABLE_PREFIX=integration_array_cache

# PostgreSQL Bit Cache Configuration
PG_BIT_CACHE_TABLE_PREFIX=integration_bit_cache
PG_BIT_CACHE_BITSIZE=200000

# PostgreSQL RoaringBit Configuration (if extension available)
PG_ROARINGBIT_CACHE_TABLE_PREFIX=integration_roaring_cache

EOF

    # Add Redis configuration if Redis service is included
    if echo "$services" | grep -q "redis-integration"; then
        cat >> .env.integration << EOF
# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6381
REDIS_CACHE_DB=0
REDIS_BIT_DB=1
REDIS_BIT_BITSIZE=200000

# Redis Set Configuration
REDIS_SET_HOST=localhost
REDIS_SET_PORT=6381
REDIS_SET_DB=0

EOF
    fi
    
    # Add MySQL configuration if MySQL service is included
    if echo "$services" | grep -q "mysql-integration"; then
        cat >> .env.integration << EOF
# MySQL Configuration (if needed)
MYSQL_HOST=localhost
MYSQL_PORT=3307
MYSQL_USER=integration_user
MYSQL_PASSWORD=integration_password
MYSQL_DATABASE=partitioncache_integration

EOF
    fi
    
    # Add RocksDB configuration (file-based, no container needed)
    # Use timestamp-based paths for fresh start
    local timestamp=$(date +%s)
    cat >> .env.integration << EOF
# RocksDB Configuration (local file storage with fresh start)
ROCKSDB_PATH=/tmp/integration_rocksdb_${timestamp}
ROCKSDB_BIT_PATH=/tmp/integration_rocksdb_bit_${timestamp}
ROCKSDB_BIT_BITSIZE=200000
ROCKSDB_DICT_PATH=/tmp/integration_rocksdict_${timestamp}

# SQLite Configuration (local file storage with fresh start)
SQLITE_DB_PATH=/tmp/integration_sqlite_${timestamp}.db

EOF
    
    echo -e "${GREEN}✅ Environment configuration created: .env.integration${NC}"
    echo -e "${YELLOW}💡 To use these settings, run:${NC}"
    echo -e "${YELLOW}   source scripts/load_env.sh && load_and_export_env${NC}"
}

# Function to test service connections
test_connections() {
    local services=$1
    echo -e "${YELLOW}🔍 Testing service connections...${NC}"
    
    # Test PostgreSQL
    if echo "$services" | grep -q "postgres-integration"; then
        if $DOCKER_COMPOSE -f $COMPOSE_FILE exec -T postgres-integration pg_isready -U integration_user -d partitioncache_integration >/dev/null 2>&1; then
            echo -e "${GREEN}✅ PostgreSQL connection test passed${NC}"
        else
            echo -e "${RED}❌ PostgreSQL connection test failed${NC}"
            return 1
        fi
    fi
    
    # Test Redis
    if echo "$services" | grep -q "redis-integration"; then
        if $DOCKER_COMPOSE -f $COMPOSE_FILE exec -T redis-integration redis-cli ping >/dev/null 2>&1; then
            echo -e "${GREEN}✅ Redis connection test passed${NC}"
        else
            echo -e "${RED}❌ Redis connection test failed${NC}"
            return 1
        fi
    fi
    
    # Test MySQL
    if echo "$services" | grep -q "mysql-integration"; then
        if $DOCKER_COMPOSE -f $COMPOSE_FILE exec -T mysql-integration mysqladmin ping -u integration_user -pintegration_password >/dev/null 2>&1; then
            echo -e "${GREEN}✅ MySQL connection test passed${NC}"
        else
            echo -e "${RED}❌ MySQL connection test failed${NC}"
            return 1
        fi
    fi
    
    echo -e "${GREEN}✅ All connection tests passed!${NC}"
}

# Function to show setup summary
show_summary() {
    local services=$1
    echo -e "\n${GREEN}🎉 Integration testing environment is ready!${NC}"
    echo -e "\n${BLUE}🔗 Service Details:${NC}"
    
    if echo "$services" | grep -q "postgres-integration"; then
        echo "  PostgreSQL: localhost:5434 (user: integration_user, db: partitioncache_integration)"
    fi
    
    if echo "$services" | grep -q "redis-integration"; then
        echo "  Redis: localhost:6381 (db 0: cache, db 1: bit cache)"
    fi
    
    if echo "$services" | grep -q "mysql-integration"; then
        echo "  MySQL: localhost:3307 (user: integration_user, db: partitioncache_integration)"
    fi
    
    echo -e "\n${YELLOW}🧪 To run integration tests:${NC}"
    echo "  source .env.integration"
    echo "  python -m pytest tests/integration/ -v"
    
    echo -e "\n${YELLOW}🎯 To run specific backend tests:${NC}"
    echo "  python -m pytest tests/integration/ -k postgresql_array"
    echo "  python -m pytest tests/integration/ -k redis_set"
    echo "  python -m pytest tests/integration/ -k rocksdb"
    
    echo -e "\n${YELLOW}🛑 To stop services:${NC}"
    echo "  ./scripts/stop_integration_testing.sh"
    
    echo -e "\n${YELLOW}📊 To view service status:${NC}"
    echo "  docker-compose -f docker-compose.integration.yml ps"
}

# Parse command line arguments
SERVICES="$DEFAULT_SERVICES"
WAIT_FOR_HEALTH=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --all-services)
            SERVICES="$ALL_SERVICES"
            shift
            ;;
        --service)
            SERVICES="$2"
            # Replace commas with spaces for docker-compose
            SERVICES=$(echo "$SERVICES" | tr ',' ' ')
            shift 2
            ;;
        --list-services)
            list_services
            ;;
        --no-wait)
            WAIT_FOR_HEALTH=false
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Main execution
main() {
    echo -e "${BLUE}🏗️  Setting up integration testing environment for PartitionCache${NC}\n"
    
    check_docker
    check_docker_compose
    start_services "$SERVICES"
    create_env_config "$SERVICES"
    test_connections "$SERVICES"
    echo -e "${YELLOW}🔨 Creating clean test database...${NC}"
    export UNIQUE_DB_NAME=partitioncache_integration
    
    # Load environment using clean loader
    source scripts/load_env.sh
    load_and_export_env .env.integration
    
    # Verify required variables are set
    verify_env_vars PG_HOST PG_PORT PG_USER PG_PASSWORD
    
    python3 scripts/create_clean_test_database.py
    show_service_status
    show_summary "$SERVICES"
}

# Run main function
main "$@"