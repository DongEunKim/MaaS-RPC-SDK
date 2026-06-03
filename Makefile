.PHONY: install test test-client test-server test-e2e test-report broker broker-stop broker-logs broker-local build clean

# 개발 환경 설치 (editable)
install:
	pip install -r requirements.txt

# 전체 테스트 (두 패키지의 pyproject.toml 설정 충돌 방지를 위해 순차 실행)
test: test-client test-server

# 패키지별 테스트
test-client:
	pytest sdk/python/client/tests/

test-server:
	pytest sdk/python/server/tests/

# E2E 통합 테스트 (mosquitto 자동 기동 포함, 브로커 수동 기동 불필요)
test-e2e:
	mkdir -p reports
	pytest tests/e2e/ -v \
	  --log-file=reports/e2e.log \
	  --log-file-level=DEBUG

# HTML 리포트 + 레이턴시 차트 생성
test-report:
	mkdir -p reports
	pytest tests/e2e/ -v \
	  --html=reports/e2e_report.html \
	  --self-contained-html \
	  --log-file=reports/e2e.log \
	  --log-file-level=DEBUG
	python3 scripts/gen_latency_chart.py
	python3 scripts/gen_throughput_chart.py || echo "throughput chart: skipped (throughput.json not found)"

# 로컬 브로커 (Docker)
broker:
	docker compose -f infra/docker/docker-compose.yml up -d --build

broker-stop:
	docker compose -f infra/docker/docker-compose.yml down

broker-logs:
	docker compose -f infra/docker/docker-compose.yml logs -f

# 로컬 브로커 (Mosquitto 직접 기동)
broker-local:
	mosquitto -c infra/mosquitto/mosquitto.conf -v

# 패키지 빌드 (.whl / .tar.gz)
build:
	cd sdk/python/client && hatch build
	cd sdk/python/server && hatch build

# 빌드 산출물 정리
clean:
	rm -rf sdk/python/client/dist sdk/python/server/dist
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
