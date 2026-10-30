#!/usr/bin/env bash
cd "$(dirname "$0")/.."; source scripts/lib.sh
./scripts/run_cluster.sh raft 5; sleep 2          # r0 wins first election (-fast)
submit 8000 "$(op 1)"; sleep 1;  echo "after op1, commit=$(commit)        (expect 1)"
fault 3 crash; fault 4 crash                       # 2 of 5 down = f
submit 8000 "$(op 2)"; sleep 1;  echo "2 crashed, commit=$(commit)        (expect 2: still live)"
fault 2 crash                                      # 3 of 5 down > f
submit 8000 "$(op 3)"; sleep 2;  echo "3 crashed, commit=$(commit)        (expect 2: stalled, no majority)"
fault 2 none; sleep 3                              # recover one -> majority again
submit 8000 "$(op 4)"; sleep 2;  echo "r2 healed, commit=$(commit)        (expect 4)"
# op3 commits together with op4 because Raft only commits old-term entries via a new current-term entry