#!/bin/bash
# Environment loader utility for PartitionCache integration testing
# Provides clean loading of .env files with proper export handling

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to load and export environment variables from a file
load_and_export_env() {
    local env_file="${1:-.env.integration}"
    
    if [[ ! -f "$env_file" ]]; then
        echo -e "${RED}❌ Environment file not found: $env_file${NC}" >&2
        return 1
    fi
    
    echo -e "${YELLOW}📝 Loading environment from: $env_file${NC}"
    
    # Read the file and export variables
    # Skip comments and empty lines
    local vars_loaded=0
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ $key =~ ^[[:space:]]*# ]] && continue
        [[ -z $key ]] && continue
        
        # Remove any leading/trailing whitespace
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        
        # Skip if key is empty after trimming
        [[ -z $key ]] && continue
        
        # Export the variable
        export "$key"="$value"
        ((vars_loaded++))
    done < "$env_file"
    
    echo -e "${GREEN}✅ Loaded and exported $vars_loaded environment variables${NC}"
    return 0
}

# Function to verify required environment variables are set
verify_env_vars() {
    local required_vars=("$@")
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        echo -e "${RED}❌ Missing required environment variables:${NC}" >&2
        printf "${RED}  - %s${NC}\n" "${missing_vars[@]}" >&2
        return 1
    fi
    
    echo -e "${GREEN}✅ All required environment variables are set${NC}"
    return 0
}

# Function to display environment status
show_env_status() {
    local vars_to_check=(
        "DB_HOST" "DB_PORT" "DB_USER" "DB_PASSWORD" "DB_NAME"
        "PG_HOST" "PG_PORT" "PG_USER" "PG_PASSWORD" "PG_DBNAME"
        "PG_QUEUE_HOST" "PG_QUEUE_PORT" "PG_QUEUE_USER" "PG_QUEUE_PASSWORD" "PG_QUEUE_DB"
        "PG_ARRAY_CACHE_TABLE_PREFIX" "PG_BIT_CACHE_TABLE_PREFIX" "PG_BIT_CACHE_BITSIZE"
        "PG_ROARINGBIT_CACHE_TABLE_PREFIX" "QUERY_QUEUE_PROVIDER"
    )
    
    echo -e "${YELLOW}📊 Environment Variables Status:${NC}"
    for var in "${vars_to_check[@]}"; do
        local value="${!var}"
        if [[ -n "$value" ]]; then
            echo -e "  ${GREEN}✓${NC} $var=$value"
        else
            echo -e "  ${RED}✗${NC} $var=(not set)"
        fi
    done
}

# If this script is being sourced, make functions available
# If it's being executed directly, provide help
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "Environment Loader Utility for PartitionCache"
    echo "============================================="
    echo ""
    echo "Usage:"
    echo "  source scripts/load_env.sh                    # Make functions available"
    echo "  source scripts/load_env.sh && load_and_export_env [env_file]"
    echo ""
    echo "Functions:"
    echo "  load_and_export_env [file]   # Load and export vars from env file (default: .env.integration)"
    echo "  verify_env_vars var1 var2... # Verify required variables are set"
    echo "  show_env_status              # Display current environment status"
    echo ""
    echo "Example:"
    echo "  source scripts/load_env.sh"
    echo "  load_and_export_env .env.integration"
    echo "  verify_env_vars DB_HOST DB_PORT DB_USER"
fi