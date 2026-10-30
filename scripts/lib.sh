submit() { # submit <port> <json>; follows one redirect
  local r to
  r=$(curl -s -X POST "localhost:$1/submit" -d "$2")
  to=$(echo "$r" | jq -r '.redirect // empty')
  [ -n "$to" ] && curl -s -X POST "$to/submit" -d "$2" >/dev/null
  return 0
}
op() { # op <n> -> a draw op
  echo "{\"kind\":\"draw\",\"id\":\"s$1\",\"lamport\":$1,\"client\":\"cli\",\"shape\":{\"type\":\"polyline\",\"points\":[[10,$((20*$1))],[250,$((20*$1+30))]],\"color\":\"#0000ff\"}}"
}
fault() { curl -s -X POST "localhost:$((8000+$1))/admin/fault?mode=$2&p=${3:-0}" >/dev/null; } # fault <idx> <mode> [p]
commit() { curl -s "localhost:8000/admin/status" | jq .commit; }
statuses() { local s=""; for ((i=0;i<$1;i++)); do s+="r$i=$(curl -s "localhost:$((8000+i))/admin/status" | jq -c '{role,commit,applied}') "; done; echo "$s"; }
replicas() { local s=""; for ((i=0;i<$1;i++)); do s+="http://localhost:$((8000+i)),"; done; echo "${s%,}"; }