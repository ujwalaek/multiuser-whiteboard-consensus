#!/usr/bin/env bash
# usage: scripts/run_cluster.sh raft 5   |   scripts/run_cluster.sh pbft 4
set -e
PROTO=$1; N=$2
cd "$(dirname "$0")/.."
./scripts/stop.sh
mkdir -p bin logs
go build -o bin/replica ./cmd/replica
go build -o bin/checker ./cmd/checker
for ((i=0;i<N;i++)); do
  FAST=""; [ "$PROTO" = raft ] && [ $i -eq 0 ] && FAST="-fast"
  ./bin/replica -id $i -n $N -proto $PROTO $FAST > logs/r$i.log 2>&1 &
  echo $! >> logs/pids
done
sleep 1
echo "cluster up: open http://localhost:8000 (and 8001, 8002, ...) in separate windows"