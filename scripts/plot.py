"""Turns results/*.jsonl into the evaluation figures (results/*.png) and a summary table."""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

R = os.path.join(os.path.dirname(__file__), "..", "results")
COL = {"raft": "#2a6fdb", "pbft": "#d9822b"}


def load(name):
    p = os.path.join(R, name)
    if not os.path.exists(p):
        return []
    rows = [json.loads(l) for l in open(p) if l.strip()]
    return [r for r in rows if r.get("p50_ms") is not None]  # a run that measured nothing


def series(rows, proto, label, x, y):
    pts = sorted((r[x], r[y]) for r in rows if r["proto"] == proto and r["label"] == label)
    return [p[0] for p in pts], [p[1] for p in pts]


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(R, name), dpi=150)
    plt.close(fig)


e1 = load("e1.jsonl")
if e1:
    fig, ax = plt.subplots(1, 3, figsize=(13, 3.8))
    for proto in ("raft", "pbft"):
        x, y = series(e1, proto, "batch=1", "n", "msgs_per_op")
        ax[0].plot(x, y, "o-", color=COL[proto], label=proto.upper())
        x, y = series(e1, proto, "batch=1", "n", "p50_ms")
        ax[1].plot(x, y, "o-", color=COL[proto], label=f"{proto.upper()}, batch 1")
        x, y = series(e1, proto, "batch=32", "n", "p50_ms")
        ax[1].plot(x, y, "s--", color=COL[proto], label=f"{proto.upper()}, batch 32")
        x, y = series(e1, proto, "batch=1", "n", "throughput")
        ax[2].plot(x, y, "o-", color=COL[proto], label=f"{proto.upper()}, batch 1")
        x, y = series(e1, proto, "batch=32", "n", "throughput")
        ax[2].plot(x, y, "s--", color=COL[proto], label=f"{proto.upper()}, batch 32")
    ax[0].set(title="Messages per operation (no batching)", xlabel="replicas N", ylabel="messages / op")
    ax[1].set(title="Median commit latency", xlabel="replicas N", ylabel="p50 latency (ms)")
    ax[2].set(title="Throughput, 8 clients", xlabel="replicas N", ylabel="ops / s")
    for a in ax:
        a.grid(alpha=0.3)
        a.legend(fontsize=8)
    save(fig, "e1_scaling.png")

e2 = load("e2.jsonl")
if e2:
    for r in e2:
        r["delay"] = int(r["label"].split("=")[1].rstrip("ms"))
    fig, ax = plt.subplots(figsize=(6, 3.8))
    for proto in ("raft", "pbft"):
        pts = sorted((r["delay"], r["p50_ms"], r["p99_ms"]) for r in e2 if r["proto"] == proto)
        ax.plot([p[0] for p in pts], [p[1] for p in pts], "o-", color=COL[proto], label=f"{proto.upper()} p50")
        ax.plot([p[0] for p in pts], [p[2] for p in pts], ":", color=COL[proto], label=f"{proto.upper()} p99")
    ax.axhline(100, color="grey", ls="--", lw=1)
    ax.text(1, 103, "100 ms responsiveness target", fontsize=8, color="grey")
    ax.set(title="Stroke commit latency vs network delay", xlabel="injected one-way delay (ms)",
           ylabel="latency (ms)")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    save(fig, "e2_delay.png")

print("\nSummary")
for name in ("e1.jsonl", "e2.jsonl", "e3.jsonl", "e5.jsonl"):
    for r in load(name):
        print(f"{name[:2]}  {r['proto']:4} n={r['n']:<2} {r['label']:<38} p50={r['p50_ms']:7.2f} ms  "
              f"p99={r['p99_ms']:7.2f} ms  {r['throughput']:7.1f} op/s  {r['msgs_per_op']:5.2f} msg/op  "
              f"max_gap={r['max_gap_ms']:6.0f} ms  timeouts={r['timeouts']}")
