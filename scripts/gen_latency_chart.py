"""reports/latency.json → reports/latency.png (레이턴시 막대 차트)"""
from __future__ import annotations
import json
import sys
from pathlib import Path

src = Path("reports/latency.json")
if not src.exists():
    print("ERROR: reports/latency.json not found. Run make test-e2e first.", file=sys.stderr)
    sys.exit(1)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

records   = json.loads(src.read_text())
names     = [r["name"].split("::")[-1] for r in records]
durations = [r["duration_s"] for r in records]
colors    = ["#4caf50" if r.get("passed") else "#f44336" for r in records]
avg       = sum(durations) / len(durations) if durations else 0

rtt_records = [r for r in records if r.get("rtt_p95_ms") is not None]

if rtt_records:
    fig, (ax, ax_rtt) = plt.subplots(
        2, 1,
        figsize=(max(12, len(names) * 0.7), 10),
        gridspec_kw={"height_ratios": [2, 1]},
    )
else:
    fig, ax = plt.subplots(figsize=(max(12, len(names) * 0.7), 6))

# 기존 막대 차트 코드 (ax 사용 부분은 그대로)
ax.barh(names, durations, color=colors)
ax.set_xlabel("Duration (s)")
ax.set_title("E2E Test Latency Distribution")
ax.axvline(x=avg, color="steelblue", linestyle="--", label=f"avg {avg:.2f}s")
ax.legend()

# RTT 서브플롯 (데이터가 있을 때만)
if rtt_records:
    rtt_names = [r["name"].split("::")[-1] for r in rtt_records]
    rtt_p95 = [r["rtt_p95_ms"] for r in rtt_records]
    ax_rtt.bar(range(len(rtt_names)), rtt_p95, color="#ff9800")
    ax_rtt.axhline(y=500.0, color="red", linestyle="--", label="SLA p95=500ms")
    ax_rtt.set_xticks(range(len(rtt_names)))
    ax_rtt.set_xticklabels(rtt_names, rotation=30, ha="right", fontsize=8)
    ax_rtt.set_ylabel("p95 RTT (ms)")
    ax_rtt.set_title("p95 RTT per Test")
    ax_rtt.legend()

plt.tight_layout()

out = Path("reports/latency.png")
fig.savefig(str(out), dpi=120)
print(f"Chart saved: {out}  ({len(records)} tests, avg {avg:.2f}s)")
