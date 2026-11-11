#!/usr/bin/env bash
# Scenario S3 (Raft, N = 5): r4 votes for every candidate and acknowledges every leader.
# Partitions {r0,r1} and {r2,r3} can each reach r4 but not each other, so each side
# elects its own leader with r4's vote and commits different ops at the same index.
# Expected: ELECTION SAFETY VIOLATION (usually) and SAFETY VIOLATION + LEDGER FORK.
cd "$(dirname "$0")/.."; source scripts/lib.sh
./scripts/stop.sh; mkdir -p bin logs; rm -f logs/submitted.jsonl
go build -o bin/replica ./cmd/replica && go build -o bin/checker ./cmd/checker && go build -o bin/sign ./cmd/sign
go run ./cmd/keygen -n 5 -dir keys 2>/dev/null
start() { ./bin/replica -id $1 -n 5 -proto raft "${@:2}" > logs/r$1.log 2>&1 & echo $! >> logs/pids; }
start 0 -fault partition -peers r0,r1,r4
start 1 -fault partition -peers r0,r1,r4
start 2 -fault partition -peers r2,r3,r4
start 3 -fault partition -peers r2,r3,r4
start 4 -fault equivocate
sleep 3
echo "leaders: $(statuses 5)"
for i in 0 1 2 3; do submit $((8000+i)) "$(op $i)"; done; sleep 1   # each side's leader takes ops
./bin/checker -once -replicas "$(replicas 5)"
RES=$?
./scripts/stop.sh
exit $RES
