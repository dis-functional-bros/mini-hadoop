#!/bin/bash

# Test script for worker failure and re-replication using test.txt
# This script uses RPC to communicate with the running master node

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Worker Failure & Re-replication Test (RPC Mode)${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Step 1: Clean up and start fresh
echo -e "${YELLOW}Step 1: Starting cluster...${NC}"
docker compose down -v 2>/dev/null || true
docker compose up --build -d
sleep 15  # Wait for cluster to be ready (increased to 15s)

# Step 2: Verify all workers are running
echo -e "\n${YELLOW}Step 2: Verifying all 4 workers are online...${NC}"
WORKER_COUNT=$(docker ps --filter "name=worker_node" --format "{{.Names}}" | wc -l)
if [ "$WORKER_COUNT" -eq 4 ]; then
    echo -e "${GREEN}✓ All 4 workers are running${NC}"
else
    echo -e "${RED}✗ Expected 4 workers, found $WORKER_COUNT${NC}"
    exit 1
fi

# Step 3: Store test file
echo -e "\n${YELLOW}Step 3: Storing test.txt to distribute blocks across workers...${NC}"

# Check if test.txt exists locally
if [ ! -f "test.txt" ]; then
    echo -e "${RED}✗ test.txt not found in current directory!${NC}"
    exit 1
fi

echo "Copying test.txt to master node..."
docker cp test.txt master_node:/tmp/test.txt

echo "Storing test.txt via RPC..."
STORE_RESULT=$(docker exec master_node elixir --sname controller --cookie secret -e '
  Node.connect(:"master@master")
  result = :rpc.call(:"master@master", MiniHadoop, :store_file, ["test.txt", "/tmp/test.txt"])
  IO.inspect(result)
')

echo "Store result: $STORE_RESULT"

if [[ "$STORE_RESULT" == *":ok"* ]]; then
    echo -e "${GREEN}✓ File stored successfully${NC}"
else
    echo -e "${RED}✗ Failed to store file${NC}"
    exit 1
fi

# Step 4: Check initial state
echo -e "\n${YELLOW}Step 4: Checking initial cluster state...${NC}"
echo "Workers before failure:"
docker ps --filter "name=worker_node" --format "table {{.Names}}\t{{.Status}}"

# Step 5: Kill a worker node to trigger :DOWN
echo -e "\n${YELLOW}Step 5: Killing worker_node1 to trigger :DOWN handler...${NC}"
docker kill worker_node1
echo -e "${RED}✗ worker_node1 killed${NC}"

# Step 6: Wait for re-replication
echo -e "\n${YELLOW}Step 6: Waiting for re-replication (20 seconds)...${NC}"
sleep 20

# Step 7: Check master logs for re-replication messages
echo -e "\n${YELLOW}Step 7: Checking master logs for re-replication activity...${NC}"
echo -e "${BLUE}--- Re-replication logs ---${NC}"
docker logs master_node 2>&1 | grep -E "(Worker.*went down|re-replication|Successfully re-replicated|Block.*replicas|removed from cluster)" | tail -20
echo -e "${BLUE}--- End of logs ---${NC}\n"

# Step 8: Verify cluster still has 3 workers
echo -e "${YELLOW}Step 8: Verifying remaining workers...${NC}"
REMAINING_WORKERS=$(docker ps --filter "name=worker_node" --format "{{.Names}}" | wc -l)
if [ "$REMAINING_WORKERS" -eq 3 ]; then
    echo -e "${GREEN}✓ 3 workers still running (expected)${NC}"
else
    echo -e "${RED}✗ Expected 3 workers, found $REMAINING_WORKERS${NC}"
fi

# Step 9: Test data integrity - retrieve file
echo -e "\n${YELLOW}Step 9: Testing data integrity by retrieving test.txt...${NC}"

RETRIEVE_RESULT=$(docker exec master_node elixir --sname controller --cookie secret -e '
  Node.connect(:"master@master")
  result = :rpc.call(:"master@master", MiniHadoop, :retrieve_file, ["test.txt"])
  IO.inspect(result)
')

echo "Retrieve result: $RETRIEVE_RESULT"

if [[ "$RETRIEVE_RESULT" == *":ok"* ]] || [[ "$RETRIEVE_RESULT" == *"File reconstructed successfully"* ]]; then
    echo -e "${GREEN}✓ test.txt retrieved successfully${NC}"
else
    echo -e "${RED}✗ Failed to retrieve test.txt${NC}"
    # Don't exit here, check for data loss first
fi

# Step 10: Check for data loss warnings
echo -e "\n${YELLOW}Step 10: Checking for data loss warnings...${NC}"
DATA_LOSS=$(docker logs master_node 2>&1 | grep -c "DATA LOST" || true)
if [ "$DATA_LOSS" -eq 0 ]; then
    echo -e "${GREEN}✓ No data loss detected${NC}"
else
    echo -e "${RED}✗ Warning: $DATA_LOSS blocks lost${NC}"
fi

# Step 11: Summary
echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Initial workers: 4"
echo -e "Workers after failure: $REMAINING_WORKERS"
echo -e "Data loss incidents: $DATA_LOSS"
echo -e "\n${GREEN}Test completed!${NC}"
echo -e "\nTo view detailed logs, run:"
echo -e "  ${BLUE}docker logs master_node${NC}"
echo -e "\nTo restart the cluster:"
echo -e "  ${BLUE}docker compose down && docker compose up -d${NC}"