#!/usr/bin/env bash
# usage: scripts/docker_delay.sh <delay, e.g. 25ms> [N]: one-way delay on every container's
# network interface (tc netem), so each message and each reply is delayed, by the kernel.
dk() { "${DOCKER:-docker}" "$@"; }   # DOCKER=path/to/docker.exe if WSL integration is off
N=${2:-$(dk ps -q --filter name='^r[0-9]+$' | wc -l)}
for ((i=0;i<N;i++)); do dk exec r$i tc qdisc replace dev eth0 root netem delay "$1"; done
