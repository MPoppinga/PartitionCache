#!/bin/bash
# Script to install PostgreSQL extensions for PartitionCache testing

set -e

echo "Installing PostgreSQL extensions for PartitionCache..."

# Function to install roaringbitmap extension
install_roaringbitmap() {
    echo "Attempting to install roaringbitmap extension..."
    
    # Get PostgreSQL version (try multiple methods)
    if command -v pg_config &> /dev/null; then
        PG_VERSION=$(pg_config --version | grep -oE '[0-9]+\.[0-9]+' | head -1 | cut -d. -f1)
        echo "Detected PostgreSQL version from pg_config: $PG_VERSION"
    else
        # Fallback: try to detect from packages
        PG_VERSION=$(dpkg -l | grep postgresql-server-dev | head -1 | grep -oE '[0-9]+' | head -1)
        if [ -z "$PG_VERSION" ]; then
            PG_VERSION=16  # Default to 16 if detection fails
        fi
        echo "Detected PostgreSQL version from packages: $PG_VERSION"
    fi
    
    # Update package lists
    sudo apt-get update
    
    # Install build dependencies
    echo "Installing build dependencies..."
    sudo apt-get install -y \
        build-essential \
        git \
        wget \
        pkg-config \
        python3-pip
    
    # Try to install PostgreSQL development packages
    if ! sudo apt-get install -y postgresql-server-dev-${PG_VERSION}; then
        echo "Warning: Could not install postgresql-server-dev-${PG_VERSION}, trying postgresql-server-dev-all"
        sudo apt-get install -y postgresql-server-dev-all || {
            echo "Warning: Could not install postgresql-server-dev packages"
        }
    fi
    
    # Install contrib package if available
    sudo apt-get install -y postgresql-contrib-${PG_VERSION} || {
        echo "Warning: Could not install postgresql-contrib-${PG_VERSION}"
    }
    
    # Try Approach 1: Use pgxn if available
    echo "Trying to install via pgxn..."
    if command -v pgxn &> /dev/null; then
        sudo pgxn install roaringbitmap && return 0
    fi
    
    # Try Approach 2: Install pgxn and use it
    echo "Installing pgxn client..."
    if ! command -v pgxn &> /dev/null; then
        sudo apt-get install -y python3-pip
        sudo pip3 install pgxnclient
    fi
    
    if command -v pgxn &> /dev/null; then
        echo "Installing roaringbitmap via pgxn..."
        sudo pgxn install roaringbitmap && return 0
    fi
    
    # Approach 3: Build from source (more robust method)
    echo "Building roaringbitmap from source..."
    
    cd /tmp
    
    # Clean up any existing directory
    sudo rm -rf pg_roaringbitmap
    
    # Clone the repository
    git clone https://github.com/ChenHuajun/pg_roaringbitmap.git
    cd pg_roaringbitmap
    
    # Ensure we have the right PostgreSQL development files
    export PG_CONFIG=$(which pg_config)
    if [ -z "$PG_CONFIG" ]; then
        export PG_CONFIG="/usr/bin/pg_config"
    fi
    
    echo "Using pg_config: $PG_CONFIG"
    
    # Build with explicit PostgreSQL configuration
    make clean || true
    make PG_CONFIG=$PG_CONFIG
    sudo make install PG_CONFIG=$PG_CONFIG
    
    echo "roaringbitmap extension built and installed from source"
    return 0
}

# Function to test extension installation
test_extension() {
    local extension_name=$1
    local db_name=${2:-test_db}
    
    echo "Testing $extension_name extension..."
    
    sudo -u postgres psql -d "$db_name" -c "CREATE EXTENSION IF NOT EXISTS $extension_name;" 2>/dev/null && {
        echo "✅ $extension_name extension is working"
        return 0
    } || {
        echo "❌ $extension_name extension failed to install"
        return 1
    }
}

# Main installation logic
main() {
    local backend=${1:-}
    
    case "$backend" in
        "postgresql_roaringbit"|"roaringbit")
            install_roaringbitmap
            test_extension "roaringbitmap"
            ;;
        "all")
            echo "Installing all available extensions..."
            install_roaringbitmap || echo "Warning: roaringbitmap installation failed"
            ;;
        *)
            echo "Usage: $0 [postgresql_roaringbit|all]"
            echo "Available extensions:"
            echo "  postgresql_roaringbit - RoaringBitmap extension"
            exit 1
            ;;
    esac
}

# Run main function with arguments
main "$@"