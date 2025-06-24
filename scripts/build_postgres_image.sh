#!/bin/bash
# Build PostgreSQL image with pg_cron and roaringbitmap extensions
# Usage: ./scripts/build_postgres_image.sh [options]

set -e

# Default values
IMAGE_NAME="postgres-test-extensions"
TAG="latest"
PUSH_TO_GHCR="false"
GITHUB_OWNER=""
VERBOSE="false"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Build PostgreSQL image with pg_cron and roaringbitmap extensions

Usage: $0 [OPTIONS]

Options:
    -h, --help              Show this help message
    -n, --name NAME         Image name (default: postgres-test-extensions)
    -t, --tag TAG           Image tag (default: latest)
    -p, --push              Push to GitHub Container Registry
    -o, --owner OWNER       GitHub username/organization (required for push)
    -v, --verbose           Verbose output
    
Examples:
    # Build image locally
    $0
    
    # Build with custom name and tag
    $0 --name my-postgres --tag v1.0
    
    # Build and push to GHCR
    $0 --push --owner yourusername
    
    # Verbose build
    $0 --verbose

Environment Variables:
    GITHUB_TOKEN            Required for pushing to GHCR (private repos)
    DOCKER_BUILDKIT         Enable Docker BuildKit (recommended)

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -n|--name)
            IMAGE_NAME="$2"
            shift 2
            ;;
        -t|--tag)
            TAG="$2"
            shift 2
            ;;
        -p|--push)
            PUSH_TO_GHCR="true"
            shift
            ;;
        -o|--owner)
            GITHUB_OWNER="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE="true"
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate requirements
if [[ "$PUSH_TO_GHCR" == "true" ]] && [[ -z "$GITHUB_OWNER" ]]; then
    print_error "GitHub owner (-o/--owner) is required when pushing to GHCR"
    exit 1
fi

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f ".github/docker/postgres-cron/Dockerfile" ]]; then
    print_error "Please run this script from the repository root"
    print_error "Expected file: .github/docker/postgres-cron/Dockerfile"
    exit 1
fi

# Set full image name
if [[ "$PUSH_TO_GHCR" == "true" ]]; then
    FULL_IMAGE_NAME="ghcr.io/${GITHUB_OWNER}/${IMAGE_NAME}:${TAG}"
else
    FULL_IMAGE_NAME="${IMAGE_NAME}:${TAG}"
fi

print_status "Building PostgreSQL image with extensions..."
print_status "Image: $FULL_IMAGE_NAME"
print_status "Context: .github/docker/postgres-cron/"

# Build arguments
BUILD_ARGS=""
if [[ "$VERBOSE" == "true" ]]; then
    BUILD_ARGS="--progress=plain"
fi

# Enable BuildKit for better performance
export DOCKER_BUILDKIT=1

# Build the image
print_status "Starting Docker build..."
if docker build $BUILD_ARGS \
    -t "$FULL_IMAGE_NAME" \
    -f .github/docker/postgres-cron/Dockerfile \
    .github/docker/postgres-cron/; then
    
    print_success "Image built successfully: $FULL_IMAGE_NAME"
else
    print_error "Docker build failed"
    exit 1
fi

# Test the image
print_status "Testing the built image..."
if docker run --rm -d \
    --name postgres-test-container \
    -e POSTGRES_DB=test_db \
    -e POSTGRES_USER=test_user \
    -e POSTGRES_PASSWORD=test_password \
    -p 5433:5432 \
    "$FULL_IMAGE_NAME" > /dev/null; then
    
    # Wait for PostgreSQL to be ready
    print_status "Waiting for PostgreSQL to start..."
    for i in {1..30}; do
        if docker exec postgres-test-container pg_isready -U test_user -d test_db > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    
    # Test extensions
    print_status "Testing PostgreSQL extensions..."
    
    # Test pg_cron
    if docker exec postgres-test-container psql -U test_user -d test_db -c "SELECT extname FROM pg_extension WHERE extname = 'pg_cron';" | grep -q pg_cron; then
        print_success "pg_cron extension is available"
    else
        print_warning "pg_cron extension not found"
    fi
    
    # Test roaringbitmap
    if docker exec postgres-test-container psql -U test_user -d test_db -c "SELECT extname FROM pg_extension WHERE extname = 'roaringbitmap';" | grep -q roaringbitmap; then
        print_success "roaringbitmap extension is available"
    else
        print_warning "roaringbitmap extension not found"
    fi
    
    # Cleanup test container
    docker stop postgres-test-container > /dev/null
    print_success "Image testing completed"
else
    print_error "Failed to start test container"
    exit 1
fi

# Push to GHCR if requested
if [[ "$PUSH_TO_GHCR" == "true" ]]; then
    print_status "Pushing to GitHub Container Registry..."
    
    # Login to GHCR if not already logged in
    if [[ -n "$GITHUB_TOKEN" ]]; then
        echo "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_OWNER" --password-stdin
    fi
    
    if docker push "$FULL_IMAGE_NAME"; then
        print_success "Image pushed to GHCR: $FULL_IMAGE_NAME"
        print_status "To use this image:"
        print_status "  docker pull $FULL_IMAGE_NAME"
    else
        print_error "Failed to push image to GHCR"
        print_error "Make sure you have the correct permissions and GITHUB_TOKEN is set"
        exit 1
    fi
fi

print_success "All operations completed successfully!"
print_status "Local image available as: $FULL_IMAGE_NAME"

if [[ "$PUSH_TO_GHCR" == "false" ]]; then
    print_status ""
    print_status "To run the image locally:"
    print_status "  docker run -d \\"
    print_status "    --name postgres-partitioncache \\"
    print_status "    -e POSTGRES_DB=your_db \\"
    print_status "    -e POSTGRES_USER=your_user \\"
    print_status "    -e POSTGRES_PASSWORD=your_password \\"
    print_status "    -p 5432:5432 \\"
    print_status "    $FULL_IMAGE_NAME"
fi 