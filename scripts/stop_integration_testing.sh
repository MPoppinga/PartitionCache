#!/bin/bash
set -e

echo "🛑 Stopping integration testing environment"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="docker-compose.integration.yml"

# Function to show help
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Stop integration testing environment for PartitionCache"
    echo ""
    echo "Options:"
    echo "  --clean, -c         Remove containers and volumes (delete all data)"
    echo "  --service SERVICE   Stop specific service(s) (comma-separated)"
    echo "  --keep-data         Stop containers but keep data volumes"
    echo "  --help, -h          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                  # Stop all services but keep data"
    echo "  $0 --clean          # Stop all services and remove all data"
    echo "  $0 --service postgres-integration  # Stop only PostgreSQL"
    echo "  $0 --service postgres-integration,redis-integration  # Stop specific services"
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

# Function to stop services
stop_services() {
    local services=$1
    local clean_data=$2
    
    cd "$(dirname "$0")/.."
    
    if [ -n "$services" ]; then
        echo -e "${YELLOW}🐳 Stopping specific services: $services${NC}"
        $DOCKER_COMPOSE -f $COMPOSE_FILE stop $services
        
        if [ "$clean_data" = true ]; then
            echo -e "${YELLOW}🧹 Removing specific containers...${NC}"
            $DOCKER_COMPOSE -f $COMPOSE_FILE rm -f $services
        fi
    else
        echo -e "${YELLOW}🐳 Stopping all integration services...${NC}"
        $DOCKER_COMPOSE -f $COMPOSE_FILE stop
        
        if [ "$clean_data" = true ]; then
            echo -e "${YELLOW}🧹 Removing all containers and volumes...${NC}"
            $DOCKER_COMPOSE -f $COMPOSE_FILE down -v
            echo -e "${GREEN}✅ Removed all containers and volumes${NC}"
        else
            echo -e "${YELLOW}💾 Data volumes preserved${NC}"
        fi
    fi
}

# Function to clean up environment files
clean_env_files() {
    local clean_data=$1
    
    if [ "$clean_data" = true ]; then
        if [ -f ".env.integration" ]; then
            rm -f .env.integration
            echo -e "${GREEN}✅ Removed .env.integration file${NC}"
        fi
    fi
}

# Function to clean up local RocksDB/SQLite data
clean_local_data() {
    local clean_data=$1
    
    if [ "$clean_data" = true ]; then
        echo -e "${YELLOW}🧹 Cleaning up local test data...${NC}"
        
        # Clean up RocksDB test directories
        if [ -d "/tmp/integration_rocksdb" ]; then
            rm -rf /tmp/integration_rocksdb
            echo -e "${GREEN}✅ Removed RocksDB test data${NC}"
        fi
        
        if [ -d "/tmp/integration_rocksdb_bit" ]; then
            rm -rf /tmp/integration_rocksdb_bit
            echo -e "${GREEN}✅ Removed RocksDB bit test data${NC}"
        fi
        
        if [ -d "/tmp/integration_rocksdict" ]; then
            rm -rf /tmp/integration_rocksdict
            echo -e "${GREEN}✅ Removed RocksDict test data${NC}"
        fi
        
        # Clean up SQLite test database
        if [ -f "/tmp/integration_sqlite.db" ]; then
            rm -f /tmp/integration_sqlite.db
            echo -e "${GREEN}✅ Removed SQLite test database${NC}"
        fi
        
        # Clean up any pytest cache
        if [ -d ".pytest_cache" ]; then
            rm -rf .pytest_cache
            echo -e "${GREEN}✅ Removed pytest cache${NC}"
        fi
    fi
}

# Function to show final status
show_final_status() {
    local clean_data=$1
    local services=$2
    
    echo -e "\n${GREEN}🎉 Integration testing environment stopped!${NC}"
    
    if [ "$clean_data" = true ]; then
        echo -e "${GREEN}🧹 All data has been cleaned up${NC}"
        echo -e "${YELLOW}💡 To start fresh: ./scripts/setup_integration_testing.sh${NC}"
    else
        echo -e "${YELLOW}💾 Data volumes preserved${NC}"
        echo -e "${YELLOW}💡 To restart: ./scripts/setup_integration_testing.sh${NC}"
        echo -e "${YELLOW}💡 To clean up data: $0 --clean${NC}"
    fi
    
    # Show current docker status
    echo -e "\n${BLUE}📊 Remaining containers:${NC}"
    if $DOCKER_COMPOSE -f $COMPOSE_FILE ps --services 2>/dev/null | grep -q .; then
        $DOCKER_COMPOSE -f $COMPOSE_FILE ps
    else
        echo "  No integration testing containers running"
    fi
}

# Parse command line arguments
CLEAN_DATA=false
SERVICES=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean|-c)
            CLEAN_DATA=true
            shift
            ;;
        --service)
            SERVICES="$2"
            # Replace commas with spaces for docker-compose
            SERVICES=$(echo "$SERVICES" | tr ',' ' ')
            shift 2
            ;;
        --keep-data)
            CLEAN_DATA=false
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
    echo -e "${BLUE}🏗️  Stopping integration testing environment for PartitionCache${NC}\n"
    
    check_docker_compose
    stop_services "$SERVICES" "$CLEAN_DATA"
    clean_env_files "$CLEAN_DATA"
    clean_local_data "$CLEAN_DATA"
    show_final_status "$CLEAN_DATA" "$SERVICES"
}

# Run main function
main "$@"