"""
고부하 병목 분석 E2E 테스트.

TC-THR-01  N=1,2,4,8,16,32 단계별 처리량·RTT 측정
TC-THR-02  병목 임계점 판정 (N >= 8이면 PASS)
TC-THR-03  성능 저하 곡선 분류 (선형/로그/지수, 지수적이면 FAIL)
TC-THR-04  SLA 검증 (p95 < 500ms, 오류율 0%, 처리량 > 10 req/s)
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import numpy as np
import pytest

from .conftest import BrokerInfo, register_rtt
from .fixtures.viss_server import VissServer
from .fixtures.viss_client import VissClient

CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32]
WARMUP_COUNT = 5
MEASURE_COUNT = 20
SLA_P95_MS = 500.0
SLA_ERROR_RATE = 0.0
SLA_THROUGHPUT_RPS = 10.0
BOTTLENECK_PASS_N = 8


@pytest.fixture(scope="module")
def throughput_server(broker: BrokerInfo):
    """모듈 전체에서 서버를 1회만 기동한다."""
    sf = VissServer(broker.host, broker.tcp_port, client_id_suffix="thr")
    sf.start()
    yield sf
    sf.stop()


@pytest.fixture(scope="module")
def throughput_results() -> dict:
    """TC-THR-01 → TC-THR-02 → TC-THR-03 → TC-THR-04 결과 전달 저장소."""
    return {}


async def _measure_concurrency(
    client: VissClient,
    n: int,
    count: int,
) -> dict:
    """N개 동시 요청을 count회 반복하여 처리량·RTT를 측정한다."""
    error_count = 0
    total_requests = n * count

    client.reset_rtt()
    t_start = time.monotonic()
    for _ in range(count):
        tasks = [
            client.set_signal("Vehicle.Speed", float(i)) for i in range(n)
        ]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        error_count += sum(1 for r in batch_results if isinstance(r, Exception))
    elapsed = time.monotonic() - t_start

    rtt_list = client.rtt_ms  # reset_rtt 이후 누적된 값
    sorted_rtt = sorted(rtt_list) if rtt_list else [0.0]
    m = len(sorted_rtt)

    return {
        "n": n,
        "throughput_rps": round(total_requests / elapsed, 3),
        "mean_rtt_ms": round(sum(sorted_rtt) / m, 2),
        "p95_rtt_ms": round(sorted_rtt[min(int(m * 0.95), m - 1)], 2),
        "error_rate": round(error_count / total_requests, 6) if total_requests else 0.0,
    }


@pytest.mark.asyncio
async def test_thr_01_sweep_concurrency(
    request,
    broker: BrokerInfo,
    throughput_server: VissServer,
    throughput_results: dict,
) -> None:
    """N=1,2,4,8,16,32 단계적 처리량·RTT 측정 후 throughput.json 저장."""
    results = []

    async with VissClient(broker.host, broker.tcp_port, "thr-sweep") as client:
        for n in CONCURRENCY_LEVELS:
            # 워밍업 (결과 버림)
            client.reset_rtt()
            warmup_tasks = [
                client.set_signal("Vehicle.Speed", 0.0) for _ in range(WARMUP_COUNT)
            ]
            await asyncio.gather(*warmup_tasks, return_exceptions=True)

            # 측정
            data = await _measure_concurrency(client, n, MEASURE_COUNT)
            results.append(data)

    throughput_results["sweep"] = results

    Path("reports").mkdir(exist_ok=True)
    (Path("reports") / "throughput.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2)
    )

    # p95 RTT 시리즈를 conftest RTT 레지스트리에 등록
    register_rtt(request.node.nodeid, [r["p95_rtt_ms"] for r in results])


@pytest.mark.asyncio
async def test_thr_02_bottleneck_threshold(throughput_results: dict) -> None:
    """처리량 증가 효율이 50% 미만으로 떨어지는 첫 N이 BOTTLENECK_PASS_N 이상이면 PASS."""
    results = throughput_results.get("sweep")
    if not results:
        pytest.skip("TC-THR-01 결과 없음")

    bottleneck_n = None
    for i in range(1, len(results)):
        prev_tp = results[i - 1]["throughput_rps"]
        curr_tp = results[i]["throughput_rps"]
        n_prev = results[i - 1]["n"]
        n_curr = results[i]["n"]
        if prev_tp > 0 and n_prev > 0:
            actual_growth = (curr_tp - prev_tp) / prev_tp
            ideal_growth = (n_curr / n_prev) - 1.0
            efficiency = actual_growth / ideal_growth if ideal_growth > 0 else 1.0
            if efficiency < 0.5 and bottleneck_n is None:
                bottleneck_n = n_curr

    if bottleneck_n is None:
        bottleneck_n = CONCURRENCY_LEVELS[-1] + 1  # 임계점 미도달

    throughput_results["bottleneck_n"] = bottleneck_n
    assert bottleneck_n >= BOTTLENECK_PASS_N, (
        f"병목 임계점 N={bottleneck_n} < 기준 {BOTTLENECK_PASS_N}"
    )


@pytest.mark.asyncio
async def test_thr_03_degradation_curve(throughput_results: dict) -> None:
    """처리량 저하가 지수적이면 FAIL. numpy polyfit으로 선형/로그/지수 피팅 후 R² 비교."""
    results = throughput_results.get("sweep")
    if not results:
        pytest.skip("TC-THR-01 결과 없음")

    x = np.array([r["n"] for r in results], dtype=float)
    y = np.array([r["throughput_rps"] for r in results], dtype=float)

    def r_squared(y_actual: np.ndarray, y_pred: np.ndarray) -> float:
        ss_res = float(np.sum((y_actual - y_pred) ** 2))
        ss_tot = float(np.sum((y_actual - np.mean(y_actual)) ** 2))
        return 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # 선형 피팅: y = a*x + b
    coef_lin = np.polyfit(x, y, 1)
    r2_lin = r_squared(y, np.polyval(coef_lin, x))

    # 로그 피팅: y = a*log(x) + b
    coef_log = np.polyfit(np.log(x), y, 1)
    r2_log = r_squared(y, coef_log[0] * np.log(x) + coef_log[1])

    # 지수 피팅: log(y) = a*x + b  (y > 0 보장을 위해 클리핑)
    y_pos = np.clip(y, 1e-6, None)
    coef_exp = np.polyfit(x, np.log(y_pos), 1)
    r2_exp = r_squared(np.log(y_pos), coef_exp[0] * x + coef_exp[1])

    curve_type = max(
        [("linear", r2_lin), ("log", r2_log), ("exp", r2_exp)],
        key=lambda t: t[1],
    )[0]

    throughput_results["curve_type"] = curve_type
    throughput_results["r2"] = {
        "linear": round(r2_lin, 4),
        "log": round(r2_log, 4),
        "exp": round(r2_exp, 4),
    }
    throughput_results["fit_coefficients"] = {
        "linear": coef_lin.tolist(),
        "log": coef_log.tolist(),
        "exp": coef_exp.tolist(),
    }

    assert curve_type != "exp", (
        f"처리량 저하가 지수적으로 분류됨 "
        f"(R²_exp={r2_exp:.3f} > R²_lin={r2_lin:.3f}, R²_log={r2_log:.3f})"
    )


@pytest.mark.asyncio
async def test_thr_04_sla_validation(throughput_results: dict) -> None:
    """p95 RTT < 500ms, 오류율 0%, 처리량 > 10 req/s — 전체 N에 대해 검증."""
    results = throughput_results.get("sweep")
    if not results:
        pytest.skip("TC-THR-01 결과 없음")

    failures: list[str] = []
    for r in results:
        n = r["n"]
        if r["p95_rtt_ms"] >= SLA_P95_MS:
            failures.append(
                f"N={n}: p95 RTT={r['p95_rtt_ms']}ms >= {SLA_P95_MS}ms"
            )
        if r["error_rate"] > SLA_ERROR_RATE:
            failures.append(
                f"N={n}: 오류율={r['error_rate']:.2%} > 0%"
            )
        # 처리량 SLA: N >= 2부터 적용 (N=1 직렬 호출은 RTT 한계로 SLA 미적용)
        if n >= 2 and r["throughput_rps"] <= SLA_THROUGHPUT_RPS:
            failures.append(
                f"N={n}: 처리량={r['throughput_rps']:.1f} req/s <= {SLA_THROUGHPUT_RPS}"
            )

    assert not failures, "SLA 위반:\n" + "\n".join(failures)
