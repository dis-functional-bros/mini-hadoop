#!/usr/bin/env bash
set -euo pipefail

MASTER=mini_hadoop_master
SUSPECT=mini_hadoop_slave2
COOKIE=mini_hadoop_secret_cookie
MASTER_NODE='master@master.node'
TMP_FILE=/tmp/rereplication.txt
DFS_FILE=repl_test.txt

step() { echo "[$(date +%H:%M:%S)] $1"; }

step "Creating dummy payload inside master"
docker exec "$MASTER" /bin/sh -c "printf 'replication check %s\n' \"\$(date)\" > $TMP_FILE"

step "Uploading to DFS through RPC"
docker exec "$MASTER" elixir --name tester@master --cookie "$COOKIE" \
  -e ':rpc.call(:"'"$MASTER_NODE"'", MiniHadoop.Client, :store_file, ["'"$DFS_FILE"'", "'"$TMP_FILE"'"]) |> IO.inspect()'

step "Sleeping to allow heartbeats to report new block"
sleep 12

step "Showing initial placements"
docker exec "$MASTER" elixir --name tester@master --cookie "$COOKIE" \
  -e ':rpc.call(:"'"$MASTER_NODE"'", MiniHadoop.Client, :cluster_info, []) |> IO.inspect(limit: :infinity)'

dockernet_stop() {
  docker stop "$1" >/dev/null
}

step "Stopping DataNode ${SUSPECT} to trigger re-replication"
dockernet_stop "$SUSPECT"
sleep 10

step "Checking file info after re-replication"
docker exec "$MASTER" elixir --name tester@master --cookie "$COOKIE" \
  -e ':rpc.call(:"'"$MASTER_NODE"'", MiniHadoop.Master.NameNode, :file_info, ["'"$DFS_FILE"'"]) |> IO.inspect(limit: :infinity)'

step "Current cluster view (post-failure)"
docker exec "$MASTER" elixir --name tester@master --cookie "$COOKIE" \
  -e ':rpc.call(:"'"$MASTER_NODE"'", MiniHadoop.Client, :cluster_info, []) |> IO.inspect(limit: :infinity)'
