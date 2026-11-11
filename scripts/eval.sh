#!/usr/bin/env bash
# Runs the evaluation (proposal Section 8) and writes results/ : one JSON line per run
# in results/e*.jsonl, scenario transcripts in results/e5_*.txt, plots via plot.py.
# usage: scripts/eval.sh [e1 e2 e3 e4 e5 e6 e7 e8]   (default: all)
cd "$(dirname "$0")/.."; source scripts/lib.sh
mkdir -p results
DUR=${DUR:-6s}
EXPS=${*:-e1 e2 e3 e4 e5 e6 e7 e8}
bench() { ./bin/bench -duration $DUR "$@"; }

for e in $EXPS; do case $e in
e1) # scaling: latency, throughput and message cost against cluster size
  rm -f results/e1.jsonl
  for batch in 1 32; do
    for cfg in "raft 3" "raft 5" "raft 7" "pbft 4" "pbft 7" "pbft 10"; do
      set -- $cfg
      ./scripts/run_cluster.sh $1 $2 -batch $batch >/dev/null; sleep 2
      bench -n $2 -clients 8 -label "batch=$batch" -out results/e1.jsonl
      ./scripts/stop.sh
    done
  done ;;
e2) # interactivity: stroke latency against injected one-way network delay
  rm -f results/e2.jsonl
  for d in 0ms 10ms 25ms 50ms; do
    for cfg in "raft 5" "pbft 4"; do
      set -- $cfg
      ./scripts/run_cluster.sh $1 $2 -delay $d >/dev/null; sleep 3
      bench -n $2 -clients 4 -label "delay=$d" -out results/e2.jsonl
      ./scripts/stop.sh
    done
  done ;;
e3) # crash and failover: how long the canvas freezes when the leader dies (5 runs each; report the median)
  rm -f results/e3.jsonl
  for run in 1 2 3 4 5; do
    ./scripts/run_cluster.sh raft 5 >/dev/null; sleep 2
    DUR=8s bench -n 5 -clients 4 -watch -2 -crash-after 3s -label "raft leader crash" -out results/e3.jsonl
    ./scripts/stop.sh
    ./scripts/run_cluster.sh pbft 4 >/dev/null; sleep 2
    DUR=8s bench -n 4 -clients 4 -watch -2 -crash-after 3s -label "pbft primary crash (view change)" -out results/e3.jsonl
    ./scripts/stop.sh
  done
  ./scripts/run_cluster.sh pbft 4 >/dev/null; sleep 2
  fault 3 crash
  DUR=8s bench -n 4 -clients 4 -label "pbft one backup crashed" -out results/e3.jsonl
  ./scripts/stop.sh ;;
e4) # partition: a 2/3 split of five Raft replicas, then healing
  ./scripts/run_cluster.sh raft 5 >/dev/null; sleep 2
  for i in 0 1 2; do curl -s -X POST "localhost:$((8000+i))/admin/fault?mode=partition&peers=r0,r1,r2" >/dev/null; done
  for i in 3 4; do curl -s -X POST "localhost:$((8000+i))/admin/fault?mode=partition&peers=r3,r4" >/dev/null; done
  for i in $(seq 1 20); do submit 8000 "$(op $i)"; done; sleep 2
  maj=$(curl -s localhost:8000/admin/status | jq .applied_ops); min=$(curl -s localhost:8003/admin/status | jq .applied_ops)
  for i in 0 1 2 3 4; do fault $i none; done
  t0=$(date +%s%N)
  until [ "$(curl -s localhost:8003/admin/status | jq .applied_ops)" = "$maj" ]; do sleep 0.02; done
  heal=$(( ($(date +%s%N) - t0) / 1000000 ))
  echo "{\"majority_ops\":$maj,\"minority_ops_during_partition\":$min,\"heal_ms\":$heal}" | tee results/e4.json
  ./bin/checker -once -replicas "$(replicas 5)" > results/e4_checker.txt
  ./scripts/stop.sh ;;
e5) # Byzantine scenarios
  ./scripts/demo_s1.sh raft > results/e5_s1_raft.txt 2>&1
  ./scripts/demo_s1.sh pbft > results/e5_s1_pbft.txt 2>&1
  ./scripts/demo_s3.sh      > results/e5_s3_raft.txt 2>&1
  ./scripts/demo_client.sh  > results/e5_client.txt 2>&1
  rm -f results/e5.jsonl
  ./scripts/run_cluster.sh pbft 4 >/dev/null; sleep 2; fault 1 equivocate
  bench -n 4 -clients 8 -label "pbft, backup r1 equivocating (S2)" -out results/e5.jsonl
  ./bin/checker -once -replicas "$(replicas 4)" > results/e5_s2_pbft.txt
  ./scripts/stop.sh ;;
e6) # authentication cost per message
  go test ./internal/common -run XXX -bench . -benchmem | tee results/e6.txt ;;
e7) # E6 on the running cluster: per-replica Ed25519 signatures against one shared HMAC key
  rm -f results/e7.jsonl
  for n in 4 7; do
    for a in ed25519 hmac; do
      ./scripts/run_cluster.sh pbft $n -auth $a -batch 1 >/dev/null; sleep 2
      bench -n $n -clients 8 -label "auth=$a" -out results/e7.jsonl
      ./scripts/stop.sh
    done
  done ;;
e8) # what durability costs: Raft with term, vote and log fsynced to disk before every reply
  rm -f results/e8.jsonl
  for d in none data; do
    flags=""; [ $d = data ] && flags="-data data"
    ./scripts/run_cluster.sh raft 5 $flags >/dev/null; sleep 2
    bench -n 5 -clients 8 -label "persist=$d" -out results/e8.jsonl
    ./scripts/stop.sh
  done
  ./scripts/demo_restart.sh > results/e8_restart.txt 2>&1 ;;
esac; done
python3 scripts/plot.py
