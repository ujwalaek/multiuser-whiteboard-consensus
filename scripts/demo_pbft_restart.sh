#!/usr/bin/env bash
# A PBFT replica killed with SIGKILL and started again with no state catches up by state
# transfer: it fetches what it missed and takes each entry f+1 replicas return identically.
# Expected: the restarted replica's commit index matches the others, and SAFETY OK on all 4.
cd "$(dirname "$0")/.."
./scripts/run_cluster.sh pbft 4 >/dev/null; sleep 2
./bin/bench -n 4 -clients 4 -duration 2s -warmup 0s -label "all 4 up" | jq -c '{label,ops}'
kill -9 "$(sed -n 4p logs/pids)"; sleep 0.5
./bin/bench -n 4 -clients 4 -duration 2s -warmup 0s -label "r3 killed" | jq -c '{label,ops}'
./bin/replica -id 3 -n 4 -proto pbft >> logs/r3.log 2>&1 & disown
sed -i "4s/.*/$!/" logs/pids; sleep 1
./bin/bench -n 4 -clients 4 -duration 3s -warmup 0s -label "r3 restarted empty" | jq -c '{label,ops}'
sleep 1
for i in 0 1 2 3; do curl -s "localhost:$((8000+i))/admin/status" | jq -c '{id,commit}'; done | tr '\n' ' '; echo
./bin/checker -once -replicas http://localhost:8000,http://localhost:8001,http://localhost:8002,http://localhost:8003
RES=$?
./scripts/stop.sh
exit $RES
