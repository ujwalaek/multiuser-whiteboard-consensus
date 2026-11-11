#!/usr/bin/env bash
# The interactivity and failover experiments again, one replica per container: delay comes
# from the kernel (tc netem) and a crash is a killed container, not a simulated one.
# Writes results/docker_e2.jsonl and results/docker_e3.jsonl.
dk() { "${DOCKER:-docker}" "$@"; }   # DOCKER=path/to/docker.exe if WSL integration is off
cd "$(dirname "$0")/.."
DUR=${DUR:-6s}
bench() { ./bin/bench -duration $DUR "$@"; }
rm -f results/docker_e2.jsonl results/docker_e3.jsonl
for d in 0ms 10ms 25ms 50ms; do
  for cfg in "raft 5" "pbft 4"; do
    set -- $cfg
    ./scripts/docker_cluster.sh $1 $2 >/dev/null; sleep 2
    [ $d != 0ms ] && ./scripts/docker_delay.sh $d $2
    bench -n $2 -clients 4 -label "docker delay=$d" -out results/docker_e2.jsonl
  done
done
for cfg in "raft 5" "pbft 4"; do
  set -- $cfg
  ./scripts/docker_cluster.sh $1 $2 >/dev/null; sleep 2
  DUR=8s bench -n $2 -clients 4 -watch -2 -label "docker: leader container killed at 3s" -out results/docker_e3.jsonl &
  sleep 4; dk kill r0 >/dev/null; wait
done
./scripts/docker_cluster.sh down
