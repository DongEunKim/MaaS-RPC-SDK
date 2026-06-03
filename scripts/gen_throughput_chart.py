"""reports/throughput.json → reports/throughput.png (처리량 분석 차트)"""
from __future__ import annotations

import json
import sys
from pathlib import Path

src = Path("reports/throughput.json")
if not src.exists():
    print(
        "ERROR: reports/throughput.json not found. Run make test-report first.",
        file=sys.stderr,
    )
    sys.exit(1)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

records = json.loads(src.read_text())
x = np.array([r["n"] for r in records], dtype=float)
y = np.array([r["throughput_rps"] for r in records], dtype=float)
p95 = np.array([r["p95_rtt_ms"] for r in records], dtype=float)

x_fine = np.linspace(x.min(), x.max(), 200)


def r_squared(y_actual: np.ndarray, y_pred: np.ndarray) -> float:
    ss_res = float(np.sum((y_actual - y_pred) ** 2))
    ss_tot = float(np.sum((y_actual - np.mean(y_actual)) ** 2))
    return 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0


# 피팅
coef_lin = np.polyfit(x, y, 1)
r2_lin = r_squared(y, np.polyval(coef_lin, x))

coef_log = np.polyfit(np.log(x), y, 1)
r2_log = r_squared(y, coef_log[0] * np.log(x) + coef_log[1])

y_pos = np.clip(y, 1e-6, None)
coef_exp = np.polyfit(x, np.log(y_pos), 1)
r2_exp = r_squared(np.log(y_pos), coef_exp[0] * x + coef_exp[1])

# 병목 임계점 계산
bottleneck_n = None
for i in range(1, len(records)):
    prev_tp = records[i - 1]["throughput_rps"]
    curr_tp = records[i]["throughput_rps"]
    n_prev = records[i - 1]["n"]
    n_curr = records[i]["n"]
    if prev_tp > 0 and n_prev > 0:
        actual_growth = (curr_tp - prev_tp) / prev_tp
        ideal_growth = (n_curr / n_prev) - 1.0
        efficiency = actual_growth / ideal_growth if ideal_growth > 0 else 1.0
        if efficiency < 0.5 and bottleneck_n is None:
            bottleneck_n = n_curr

fig, ax1 = plt.subplots(figsize=(10, 6))

# 실측값
ax1.scatter(x, y, color="steelblue", zorder=5, label="Measured throughput")

# 피팅 곡선
ax1.plot(
    x_fine, np.polyval(coef_lin, x_fine),
    "b--", alpha=0.7, label=f"Linear fit (R²={r2_lin:.3f})",
)
ax1.plot(
    x_fine, coef_log[0] * np.log(x_fine) + coef_log[1],
    "g-.", alpha=0.7, label=f"Log fit (R²={r2_log:.3f})",
)
ax1.plot(
    x_fine, np.exp(coef_exp[0] * x_fine + coef_exp[1]),
    "r:", alpha=0.7, label=f"Exp fit (R²={r2_exp:.3f})",
)

# 병목 임계점
if bottleneck_n is not None:
    ax1.axvline(
        x=bottleneck_n, color="orange", linestyle="--",
        label=f"Bottleneck N={bottleneck_n}",
    )

ax1.set_xlabel("Concurrency (N)")
ax1.set_ylabel("Throughput (req/s)")
ax1.set_title("Throughput vs Concurrency (N)")
ax1.set_xscale("log", base=2)
ax1.set_xticks(x)
ax1.set_xticklabels([str(int(v)) for v in x])

# p95 RTT 보조 y축
ax2 = ax1.twinx()
ax2.plot(x, p95, "o-", color="darkorange", alpha=0.6, label="p95 RTT")
ax2.axhline(y=500.0, color="red", linestyle=":", alpha=0.5, label="SLA p95=500ms")
ax2.set_ylabel("p95 RTT (ms)", color="darkorange")
ax2.tick_params(axis="y", labelcolor="darkorange")

# 범례 통합
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8)

plt.tight_layout()
out = Path("reports/throughput.png")
fig.savefig(str(out), dpi=120)
print(f"Chart saved: {out}  ({len(records)} levels, bottleneck_n={bottleneck_n})")
