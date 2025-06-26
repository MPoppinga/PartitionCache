#!/bin/bash
set -e

echo "🧪 Comprehensive Integration Test Runner"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Default settings
RUN_SETUP=true
RUN_CLEANUP=false
TEST_PATTERN=""
VERBOSE=false
CI_MODE=false
SERVICES="postgres-integration redis-integration"
PARALLEL=false
COVERAGE=false
PYTEST_ARGS=""
IGNORE_PATHS=""

# Function to show help
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Run comprehensive integration tests with proper environment setup"
    echo ""
    echo "Options:"
    echo "  --no-setup           Skip container setup (assume already running)"
    echo "  --cleanup            Clean up containers after tests"
    echo "  --pattern PATTERN    Test pattern to run (e.g., 'postgresql', 'redis', 'rocksdb')"
    echo "  --backend BACKEND    Test specific backend (e.g., 'postgresql_array', 'redis_set')"
    echo "  --services SERVICES  Comma-separated list of services to start"
    echo "  --all-services       Start all available services"
    echo "  --verbose, -v        Run tests in verbose mode"
    echo "  --parallel, -n       Run tests in parallel (using pytest-xdist)"
    echo "  --coverage           Generate coverage report"
    echo "  --ci                 Run in CI mode (uses different setup)"
    echo "  --ignore PATH        Ignore path/file from tests (can be used multiple times)"
    echo "  --list-backends      List all available cache backends"
    echo "  --pytest-args ARGS   Pass additional arguments to pytest"
    echo "  --help, -h           Show this help message"
    echo ""
    echo "Test Patterns:"
    echo "  postgresql           All PostgreSQL-based backends"
    echo "  redis               All Redis-based backends"
    echo "  rocksdb             All RocksDB-based backends"
    echo "  queue               Queue processing tests"
    echo "  performance         Performance tests"
    echo "  error_recovery      Error recovery tests"
    echo ""
    echo "Examples:"
    echo "  $0                              # Run all tests with default services"
    echo "  $0 --all-services              # Run all tests with all services"
    echo "  $0 --pattern postgresql         # Run only PostgreSQL backend tests"
    echo "  $0 --backend redis_set          # Run only redis_set backend tests"
    echo "  $0 --cleanup --coverage         # Run tests with cleanup and coverage"
    echo "  $0 --no-setup --pattern queue   # Run queue tests assuming services are running"
    echo "  $0 --ci --parallel              # Run in CI mode with parallel execution"
    echo "  $0 --ignore=tests/integration/test_pg_cron_integration.py # Exclude pg_cron tests"
}

# Function to list available backends
list_backends() {
    echo -e "${BLUE}📋 Available Cache Backends:${NC}"
    echo ""
    echo -e "${PURPLE}PostgreSQL-based:${NC}"
    echo "  postgresql_array      - PostgreSQL with array storage"
    echo "  postgresql_bit        - PostgreSQL with bit storage"
    echo "  postgresql_roaringbit - PostgreSQL with RoaringBitmap extension"
    echo ""
    echo -e "${PURPLE}Redis-based:${NC}"
    echo "  redis_set             - Redis with set storage"
    echo "  redis_bit             - Redis with bitmap storage"
    echo ""
    echo -e "${PURPLE}RocksDB-based:${NC}"
    echo "  rocksdb_set           - RocksDB with set storage"
    echo "  rocksdb_bit           - RocksDB with bitmap storage"
    echo "  rocksdict             - RocksDict (pip-installable RocksDB)"
    echo ""
    echo -e "${PURPLE}File-based:${NC}"
    echo "  sqlite                - SQLite database handler"
    echo ""
    echo -e "${YELLOW}Note: Backend availability depends on installed dependencies and running services${NC}"
    exit 0
}

# Function to setup integration environment
setup_integration_env() {
    echo -e "${BLUE}🏠 Setting up integration testing environment...${NC}"
    
    if [ "$CI_MODE" = true ]; then
        echo -e "${BLUE}🤖 CI Mode: Using service containers${NC}"
        # In CI mode, services are typically provided by the CI system
        # Set up environment variables for CI
        export PG_HOST=${PG_HOST}
        export PG_PORT=${PG_PORT}
        export PG_USER=${PG_USER}
        export PG_PASSWORD=${PG_PASSWORD}
        export PG_DBNAME=${PG_DBNAME}
        
        export REDIS_HOST=${REDIS_HOST:-localhost}
        export REDIS_PORT=${REDIS_PORT:-6379}
        export REDIS_CACHE_DB=${REDIS_CACHE_DB:-0}
        export REDIS_BIT_DB=${REDIS_BIT_DB:-1}
    else
        # Local mode: use our Docker Compose setup
        ./scripts/setup_integration_testing.sh --service "$SERVICES"
    fi
}

# Function to load environment variables
load_env() {
    if [ "$CI_MODE" = true ]; then
        echo -e "${YELLOW}📝 Using CI environment variables${NC}"
    else
        if [ -f ".env.integration" ]; then
            echo -e "${YELLOW}📝 Loading integration environment variables...${NC}"
            # Export all non-comment lines from .env.integration
            export $(grep -v '^#' .env.integration | grep '=' | xargs)
        else
            echo -e "${YELLOW}⚠️  No .env.integration file found, using defaults${NC}"
        fi
    fi
}

# Function to run the tests
run_tests() {
    echo -e "${YELLOW}🧪 Running integration tests...${NC}"
    
    # Construct pytest command
    PYTEST_CMD="python -m pytest tests/integration/"
    
    # Add pattern filter
    if [ -n "$TEST_PATTERN" ]; then
        if [ "$TEST_PATTERN" = "postgresql" ]; then
            PYTEST_CMD="$PYTEST_CMD -k 'postgresql_array or postgresql_bit or postgresql_roaringbit'"
        elif [ "$TEST_PATTERN" = "redis" ]; then
            PYTEST_CMD="$PYTEST_CMD -k 'redis_set or redis_bit'"
        elif [ "$TEST_PATTERN" = "rocksdb" ]; then
            PYTEST_CMD="$PYTEST_CMD -k 'rocksdb_set or rocksdb_bit or rocksdict'"
        else
            PYTEST_CMD="$PYTEST_CMD -k $TEST_PATTERN"
        fi
    fi
    
    # Add specific backend filter
    if [ -n "$BACKEND_FILTER" ]; then
        if [ -n "$TEST_PATTERN" ]; then
            PYTEST_CMD="$PYTEST_CMD and $BACKEND_FILTER"
        else
            PYTEST_CMD="$PYTEST_CMD -k $BACKEND_FILTER"
        fi
    fi
    
    # Add verbose flag
    if [ "$VERBOSE" = true ]; then
        PYTEST_CMD="$PYTEST_CMD -v"
    fi
    
    # Add parallel execution
    if [ "$PARALLEL" = true ]; then
        PYTEST_CMD="$PYTEST_CMD -n auto"
    fi
    
    # Add coverage
    if [ "$COVERAGE" = true ]; then
        PYTEST_CMD="$PYTEST_CMD --cov=partitioncache --cov-report=html --cov-report=term-missing"
    fi
    
    # Add ignore paths
    if [ -n "$IGNORE_PATHS" ]; then
        PYTEST_CMD="$PYTEST_CMD $IGNORE_PATHS"
    fi
    
    # Add extra pytest args
    if [ -n "$PYTEST_ARGS" ]; then
        PYTEST_CMD="$PYTEST_CMD $PYTEST_ARGS"
    fi
    
    # Show environment info
    echo -e "${BLUE}📋 Test Environment:${NC}"
    echo "  PostgreSQL: ${PG_HOST:-localhost}:${PG_PORT:-5432}"
    echo "  Redis: ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
    echo "  Services: $SERVICES"
    echo "  Pattern: ${TEST_PATTERN:-all}"
    echo "  Backend: ${BACKEND_FILTER:-all}"
    echo ""
    echo -e "${BLUE}🏃 Running command: $PYTEST_CMD${NC}"
    echo ""
    
    # Show available backends
    show_available_backends
    
    # Run the tests
    local start_time=$(date +%s)
    
    if eval $PYTEST_CMD; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "\n${GREEN}✅ All integration tests passed! (${duration}s)${NC}"
        
        if [ "$COVERAGE" = true ]; then
            echo -e "${BLUE}📊 Coverage report generated in htmlcov/${NC}"
        fi
        
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "\n${RED}❌ Some integration tests failed (${duration}s)${NC}"
        return 1
    fi
}

# Function to show available backends
show_available_backends() {
    echo -e "${BLUE}🔍 Detecting available backends...${NC}"
    
    # Check PostgreSQL
    if [ -n "$PG_HOST" ]; then
        echo -e "${GREEN}  ✅ PostgreSQL backends available${NC}"
    else
        echo -e "${YELLOW}  ⚠️  PostgreSQL backends not configured${NC}"
    fi
    
    # Check Redis
    if [ -n "$REDIS_HOST" ]; then
        echo -e "${GREEN}  ✅ Redis backends available${NC}"
    else
        echo -e "${YELLOW}  ⚠️  Redis backends not configured${NC}"
    fi
    
    # Check RocksDB
    if python -c "import rocksdb" 2>/dev/null; then
        echo -e "${GREEN}  ✅ RocksDB backends available${NC}"
    else
        echo -e "${YELLOW}  ⚠️  RocksDB backends not available (module not installed)${NC}"
    fi
    
    # Check rocksdict
    if python -c "import rocksdict" 2>/dev/null; then
        echo -e "${GREEN}  ✅ RocksDict backend available${NC}"
    else
        echo -e "${YELLOW}  ⚠️  RocksDict backend not available${NC}"
    fi
    
    echo ""
}

# Function to cleanup integration environment
cleanup_integration_env() {
    echo -e "${YELLOW}🧹 Cleaning up integration testing environment...${NC}"
    if [ "$CI_MODE" = true ]; then
        echo -e "${BLUE}🤖 CI mode: Skipping manual cleanup${NC}"
    else
        ./scripts/stop_integration_testing.sh --clean
    fi
}

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}🔍 Checking prerequisites...${NC}"
    
    # Check if we're in the right directory
    if [ ! -f "pyproject.toml" ] || [ ! -d "tests/integration" ]; then
        echo -e "${RED}❌ This script must be run from the PartitionCache root directory${NC}"
        exit 1
    fi
    
    # Check Python and pytest
    if ! command -v python >/dev/null 2>&1; then
        echo -e "${RED}❌ Python is not available${NC}"
        exit 1
    fi
    
    if ! python -m pytest --version >/dev/null 2>&1; then
        echo -e "${RED}❌ pytest is not available${NC}"
        exit 1
    fi
    
    # Check pytest-xdist if parallel mode is requested
    if [ "$PARALLEL" = true ]; then
        if ! python -c "import xdist" 2>/dev/null; then
            echo -e "${YELLOW}⚠️  pytest-xdist not available, disabling parallel mode${NC}"
            PARALLEL=false
        fi
    fi
    
    # Check coverage plugin if coverage is requested
    if [ "$COVERAGE" = true ]; then
        if ! python -c "import pytest_cov" 2>/dev/null; then
            echo -e "${YELLOW}⚠️  pytest-cov not available, disabling coverage${NC}"
            COVERAGE=false
        fi
    fi
    
    # Check Docker if not in CI mode and setup is requested
    if [ "$CI_MODE" != true ] && [ "$RUN_SETUP" = true ]; then
        if ! command -v docker >/dev/null 2>&1; then
            echo -e "${RED}❌ Docker is not available${NC}"
            exit 1
        fi
        
        if ! docker info >/dev/null 2>&1; then
            echo -e "${RED}❌ Docker is not running${NC}"
            exit 1
        fi
    fi
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-setup)
            RUN_SETUP=false
            shift
            ;;
        --cleanup)
            RUN_CLEANUP=true
            shift
            ;;
        --pattern)
            TEST_PATTERN="$2"
            shift 2
            ;;
        --backend)
            BACKEND_FILTER="$2"
            shift 2
            ;;
        --services)
            SERVICES="$2"
            # Replace commas with spaces
            SERVICES=$(echo "$SERVICES" | tr ',' ' ')
            shift 2
            ;;
        --all-services)
            SERVICES="postgres-integration redis-integration mysql-integration"
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --parallel|-n)
            PARALLEL=true
            shift
            ;;
        --coverage)
            COVERAGE=true
            shift
            ;;
        --ci)
            CI_MODE=true
            shift
            ;;
        --ignore)
            if [ -n "$IGNORE_PATHS" ]; then
                IGNORE_PATHS="$IGNORE_PATHS --ignore=$2"
            else
                IGNORE_PATHS="--ignore=$2"
            fi
            shift 2
            ;;
        --ignore=*)
            IGNORE_PATH="${1#*=}"
            if [ -n "$IGNORE_PATHS" ]; then
                IGNORE_PATHS="$IGNORE_PATHS --ignore=$IGNORE_PATH"
            else
                IGNORE_PATHS="--ignore=$IGNORE_PATH"
            fi
            shift
            ;;
        --list-backends)
            list_backends
            ;;
        --pytest-args)
            PYTEST_ARGS="$2"
            shift 2
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
    echo -e "${BLUE}🚀 Starting Comprehensive Integration Test Runner${NC}\n"
    
    # Change to script directory's parent (project root)
    cd "$(dirname "$0")/.."
    
    check_prerequisites
    
    # Setup integration environment if requested
    if [ "$RUN_SETUP" = true ]; then
        setup_integration_env
    fi
    
    # Load environment variables
    load_env
    
    # Run the tests
    if run_tests; then
        TEST_RESULT=0
    else
        TEST_RESULT=1
    fi
    
    # Cleanup if requested
    if [ "$RUN_CLEANUP" = true ]; then
        cleanup_integration_env
    fi
    
    # Final status
    if [ $TEST_RESULT -eq 0 ]; then
        echo -e "\n${GREEN}🎉 Integration testing completed successfully!${NC}"
        if [ "$RUN_CLEANUP" != true ] && [ "$CI_MODE" != true ]; then
            echo -e "${YELLOW}💡 To stop services: ./scripts/stop_integration_testing.sh${NC}"
        fi
    else
        echo -e "\n${RED}💥 Integration testing failed${NC}"
        if [ "$RUN_CLEANUP" != true ] && [ "$CI_MODE" != true ]; then
            echo -e "${YELLOW}💡 Services are still running for debugging${NC}"
            echo -e "${YELLOW}💡 To stop: ./scripts/stop_integration_testing.sh${NC}"
        fi
        exit 1
    fi
}

# Run main function
main "$@"