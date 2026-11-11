#!/usr/bin/env bash
# Real crash and restart. Raft replicas keep term, vote and log on disk (-data), so a
# replica killed with SIGKILL comes back with everything it acknowledged.
# Expected: a killed and restarted replica catches up; after the whole cluster is killed
# and restarted, no committed stroke is lost and every ledger matches.
cd "$(dirname "$0")/.."; source scripts/lib.sh
N=3
./scripts/run_cluster.sh raft $N -data data >/dev/null; sleep 2
start() { # start <i>: run replica i again from its data directory, recording its pid
  ./bin/replica -id $1 -n $N -proto raft -data data >> logs/r$1.log 2>&1 &
  disown
  sed -i "$(( $1 + 1 ))s/.*/$!/" logs/pids
}
kill9() { kill -9 "$(sed -n "$(( $1 + 1 ))p" logs/pids)"; }
for i in 1 2 3 4 5; do submit 8000 "$(op $i)"; done; sleep 1
echo "5 strokes:          $(statuses $N)"
kill9 2; sleep 0.5; start 2; sleep 2
for i in 6 7; do submit 8000 "$(op $i)"; done; sleep 1
echo "r2 killed, back:    $(statuses $N)"
for i in 0 1 2; do kill9 $i; done; sleep 0.5
for i in 0 1 2; do start $i; done; sleep 3
submit 8000 "$(op 8)"; sleep 1        # a stroke in the new term commits everything before it
echo "all killed, back:   $(statuses $N)"
./bin/checker -once -replicas "$(replicas $N)"
RES=$?
./scripts/stop.sh
exit $RES
