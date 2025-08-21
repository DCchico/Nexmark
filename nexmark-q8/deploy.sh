#!/bin/bash

# Nexmark Query 8 Deployment Script
# This script builds and deploys the function with the specified Redis server

set -e

echo "🚀 Deploying Nexmark Query 8 - Streaming Implementation"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Redis configuration
REDIS_HOST="128.110.217.212"
REDIS_PORT="6379"
REDIS_DB="0"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if OpenFaaS CLI is installed
if ! command -v faas-cli &> /dev/null; then
    print_error "OpenFaaS CLI is not installed. Please install it first."
    exit 1
fi

# Step 1: Test Redis connection
print_status "Testing Redis connection to ${REDIS_HOST}:${REDIS_PORT}..."
if command -v redis-cli &> /dev/null; then
    if redis-cli -h $REDIS_HOST -p $REDIS_PORT ping | grep -q PONG; then
        print_status "Redis connection successful"
    else
        print_error "Redis connection failed. Please check if Redis is running on ${REDIS_HOST}:${REDIS_PORT}"
        exit 1
    fi
else
    print_warning "redis-cli not found, skipping connection test"
    print_warning "Please ensure Redis is running on ${REDIS_HOST}:${REDIS_PORT}"
fi

# Step 2: Build the function
print_status "Building Nexmark Query 8 function..."
faas-cli build nexmark-q8

if [ $? -eq 0 ]; then
    print_status "Function built successfully"
else
    print_error "Function build failed"
    exit 1
fi

# Step 3: Deploy the function with Redis configuration
print_status "Deploying function to OpenFaaS with Redis server ${REDIS_HOST}..."
faas-cli deploy nexmark-q8 --env REDIS_HOST=$REDIS_HOST --env REDIS_PORT=$REDIS_PORT --env REDIS_DB=$REDIS_DB

if [ $? -eq 0 ]; then
    print_status "Function deployed successfully"
else
    print_error "Function deployment failed"
    exit 1
fi

# Step 4: Test the function
print_status "Testing function with sample data..."

# Test with a simple bid event
TEST_EVENT='{
  "stream_id": "deploy_test",
  "sequence_id": '$(date +%s%3N)',
  "bid_event": {
    "person": {
      "id": "person_deploy_test",
      "name": "Deploy Test Person",
      "email": "deploy@test.com"
    },
    "auction": {
      "id": "auction_deploy_test",
      "seller_id": "person_deploy_test",
      "reserve": 100.0,
      "type": "Regular"
    },
    "bid_amount": 150.0,
    "bid_time": '$(date +%s)'
  }
}'

echo "$TEST_EVENT" | faas-cli invoke nexmark-q8

if [ $? -eq 0 ]; then
    print_status "Function test successful"
else
    print_warning "Function test failed - this might be normal if Redis connection is not configured correctly"
fi

# Step 5: Show usage information
echo ""
echo "🎉 Deployment completed!"
echo "========================"
echo ""
echo "Configuration:"
echo "  Redis Host: $REDIS_HOST"
echo "  Redis Port: $REDIS_PORT"
echo "  Redis DB: $REDIS_DB"
echo ""
echo "Usage examples:"
echo ""
echo "1. Test with bid event:"
echo "   echo '{\"stream_id\": \"test_stream\", \"sequence_id\": 123456, \"bid_event\": {\"person\": {\"id\": \"person_1\", \"name\": \"John\"}, \"auction\": {\"id\": \"auction_1\", \"seller_id\": \"person_1\", \"reserve\": 100}}}' | faas-cli invoke nexmark-q8"
echo ""
echo "2. Initialize 12-hour window:"
echo "   echo '{\"command\": \"initialize\", \"stream_id\": \"test_stream\"}' | faas-cli invoke nexmark-q8"
echo ""
echo "3. Check window status:"
echo "   echo '{\"command\": \"status\", \"stream_id\": \"test_stream\"}' | faas-cli invoke nexmark-q8"
echo ""
echo "4. Test with XML data:"
echo "   echo '{\"xml_data\": \"<?xml version=\\\"1.0\\\"?><site><person id=\\\"person_1\\\"><name>John</name></person><open_auctions><open_auction id=\\\"auction_1\\\"><seller><person_ref person=\\\"person_1\\\"/></seller><reserve>100</reserve></open_auction></open_auctions></site>\"}' | faas-cli invoke nexmark-q8"
echo ""
echo "5. Local testing (before deployment):"
echo "   python local_test.py"
echo ""
echo "6. View function logs:"
echo "   faas-cli logs nexmark-q8"
echo ""
echo "7. Check Redis data:"
echo "   redis-cli -h $REDIS_HOST -p $REDIS_PORT keys 'nexmark:*'"
echo ""

print_status "Function is ready to process streaming auction data!"
print_status "Redis server: ${REDIS_HOST}:${REDIS_PORT}" 