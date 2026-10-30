#!/usr/bin/env bash
# A Byzantine backup (r1) lies to the clients connected to it.
# Expected: a client trusting only r1 draws tampered shapes; a PBFT client that needs
# f+1 = 2 matching replies from all replicas draws only true shapes and flags r1.
cd "$(dirname "$0")/.."; source scripts/lib.sh
source ~/.nvm/nvm.sh >/dev/null 2>&1
./scripts/run_cluster.sh pbft 4 >/dev/null; sleep 2
fault 1 equivocate
for i in 1 2 3 4 5; do submit 8000 "$(op $i)"; done; sleep 2
node scripts/client_view.js 4 1 1      # trusts r1 alone
node scripts/client_view.js 4 1 2      # trusts honest r2 alone
node scripts/client_view.js 4 2        # PBFT client: f+1 of all 4
./scripts/stop.sh
