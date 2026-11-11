#!/usr/bin/env bash
# Scenario S1: the leader/primary (r0) equivocates.
# usage: scripts/demo_s1.sh raft|pbft
# Expected: raft -> SAFETY VIOLATION (honest replicas diverge)
#           pbft -> SAFETY OK: the lie is caught, signed proof triggers a view change to r1,
#                   and the client's re-sent strokes commit in view 1
cd "$(dirname "$0")/.."; source scripts/lib.sh
PROTO=$1; N=4
./scripts/run_cluster.sh "$PROTO" $N >/dev/null; sleep 2
submit 8000 "$(op 1)"; sleep 1
echo "before fault: $(statuses $N)"
fault 0 equivocate                                 # r0 tells odd-numbered peers a different story
submit 8000 "$(op 2)"; submit 8000 "$(op 3)"; sleep 2
echo "after fault:  $(statuses $N)"
if [ "$PROTO" = pbft ]; then   # strokes 2 and 3 never committed: the client sends them to every replica
  submit_all $N "$(op 2)"; submit_all $N "$(op 3)"; sleep 2
  echo "after resend: $(statuses $N)"
fi
./bin/checker -once -replicas "$(replicas $N)"
RES=$?
./scripts/stop.sh
exit $RES
