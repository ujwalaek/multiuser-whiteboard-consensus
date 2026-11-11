#!/usr/bin/env bash
# One replica per container, on a real (virtual) network instead of localhost.
# usage: scripts/docker_cluster.sh raft|pbft N [replica flags...]   then: scripts/docker_cluster.sh down
# Containers are named r0..r(N-1); HTTP is published on localhost:8000+i as before, so the
# browser, bench and checker work unchanged. Faults are real: docker kill r1 (crash),
# docker network disconnect wb_default r1 (partition), scripts/docker_delay.sh 25ms (delay).
dk() { "${DOCKER:-docker}" "$@"; }   # DOCKER=path/to/docker.exe if WSL integration is off
set -e
cd "$(dirname "$0")/.."
if [ "$1" = down ]; then dk compose down -t 1 >/dev/null 2>&1; exit 0; fi
PROTO=$1; N=$2; shift 2
./scripts/stop.sh
dk compose down -t 1 >/dev/null 2>&1 || true
mkdir -p bin logs; rm -f logs/submitted.jsonl
go run ./cmd/keygen -n $N -dir keys 2>/dev/null
go build -o bin/bench ./cmd/bench; go build -o bin/checker ./cmd/checker; go build -o bin/sign ./cmd/sign
addrs=""; for ((i=0;i<N;i++)); do addrs+="r$i:$((7000+i)),"; done; addrs=${addrs%,}
extra=""; for f in "$@"; do extra+=", \"$f\""; done
{
  echo "name: wb"
  echo "services:"
  for ((i=0;i<N;i++)); do
    fast=""; [ "$PROTO" = raft ] && [ $i -eq 0 ] && fast=', "-fast"'
    cat <<YML
  r$i:
    image: wb-replica
    container_name: r$i
    command: ["-id", "$i", "-n", "$N", "-proto", "$PROTO", "-addrs", "$addrs"$fast$extra]
    ports: ["$((8000+i)):$((8000+i))"]
    cap_add: [NET_ADMIN]
YML
  done
} > docker-compose.yml
dk build -q -t wb-replica . >/dev/null
dk compose up -d >/dev/null
for ((i=0;i<N;i++)); do until curl -sf "localhost:$((8000+i))/admin/status" >/dev/null; do sleep 0.2; done; done
echo "docker cluster up: $N $PROTO replicas in containers r0..r$((N-1)), http://localhost:8000"
