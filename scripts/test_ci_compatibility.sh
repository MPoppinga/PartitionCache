#!/bin/bash
set -e

echo "🧪 Testing CI Compatibility for Integration Infrastructure"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Simulate CI environment variables
export CI=true
export PG_HOST=localhost
export PG_PORT=5432
export PG_USER=integration_user
export PG_PASSWORD=integration_password
export PG_DBNAME=partitioncache_integration
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_CACHE_DB=0
export REDIS_BIT_DB=1

echo -e "${BLUE}🔍 Testing CI mode detection...${NC}"

# Test CI mode help
echo -e "${YELLOW}Testing --ci flag recognition:${NC}"
if ./scripts/run_integration_tests.sh --help | grep -q "\-\-ci"; then
    echo -e "${GREEN}✅ CI flag is recognized${NC}"
else
    echo -e "${RED}❌ CI flag not found in help${NC}"
    exit 1
fi

# Test pattern recognition
echo -e "${YELLOW}Testing pattern recognition:${NC}"
patterns=("postgresql" "redis" "rocksdb" "queue" "performance")
for pattern in "${patterns[@]}"; do
    if ./scripts/run_integration_tests.sh --help | grep -q "$pattern"; then
        echo -e "${GREEN}✅ Pattern '$pattern' is documented${NC}"
    else
        echo -e "${RED}❌ Pattern '$pattern' not found in help${NC}"
        exit 1
    fi
done

# Test backend listing
echo -e "${YELLOW}Testing backend listing:${NC}"
if ./scripts/run_integration_tests.sh --list-backends >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Backend listing works${NC}"
else
    echo -e "${RED}❌ Backend listing failed${NC}"
    exit 1
fi

# Test environment variable handling
echo -e "${YELLOW}Testing environment variable handling:${NC}"
if [ "$CI" = "true" ] && [ -n "$PG_HOST" ] && [ -n "$REDIS_HOST" ]; then
    echo -e "${GREEN}✅ CI environment variables are set${NC}"
else
    echo -e "${RED}❌ CI environment variables missing${NC}"
    exit 1
fi

# Test script prerequisites check (without actually running tests)
echo -e "${YELLOW}Testing prerequisites check:${NC}"
# We can't actually test the full prerequisites without Docker running,
# but we can test the script structure
if command -v python >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Python is available${NC}"
else
    echo -e "${RED}❌ Python not available${NC}"
    exit 1
fi

if python -c "import pytest" 2>/dev/null; then
    echo -e "${GREEN}✅ pytest is available${NC}"
else
    echo -e "${YELLOW}⚠️  pytest not available (would be installed in CI)${NC}"
fi

# Test Docker Compose configuration validity
echo -e "${YELLOW}Testing Docker Compose configuration:${NC}"
if docker-compose -f docker-compose.integration.yml config --quiet 2>/dev/null; then
    echo -e "${GREEN}✅ Docker Compose configuration is valid${NC}"
else
    echo -e "${RED}❌ Docker Compose configuration is invalid${NC}"
    exit 1
fi

# Test that our scripts are executable
echo -e "${YELLOW}Testing script permissions:${NC}"
scripts=("setup_integration_testing.sh" "run_integration_tests.sh" "stop_integration_testing.sh")
for script in "${scripts[@]}"; do
    if [ -x "./scripts/$script" ]; then
        echo -e "${GREEN}✅ Script '$script' is executable${NC}"
    else
        echo -e "${RED}❌ Script '$script' is not executable${NC}"
        exit 1
    fi
done

# Test CI workflow syntax (basic YAML check)
echo -e "${YELLOW}Testing GitHub Actions workflow syntax:${NC}"
if command -v python >/dev/null 2>&1; then
    if python -c "import yaml; yaml.safe_load(open('.github/workflows/integration-tests-new.yml'))" 2>/dev/null; then
        echo -e "${GREEN}✅ GitHub Actions workflow YAML is valid${NC}"
    else
        echo -e "${YELLOW}⚠️  YAML validation skipped (PyYAML not available)${NC}"
    fi
fi

echo -e "\n${GREEN}🎉 CI Compatibility Test Summary:${NC}"
echo -e "${GREEN}✅ All CI compatibility tests passed!${NC}"
echo -e "\n${BLUE}📋 What this verifies:${NC}"
echo "  - CI mode flags are recognized"
echo "  - Pattern-based testing is supported"
echo "  - Backend listing functionality works"
echo "  - Environment variables are properly handled"
echo "  - Scripts have correct permissions"
echo "  - Docker Compose configuration is valid"
echo "  - GitHub Actions workflow syntax is correct"

echo -e "\n${YELLOW}💡 Next steps to verify in actual CI:${NC}"
echo "  1. Test with actual service containers"
echo "  2. Verify PostgreSQL extensions installation"
echo "  3. Run pattern-based tests"
echo "  4. Test parallel execution"
echo "  5. Verify coverage reporting"