#!/usr/bin/env bash
# MaaS Python SDK 설치 스크립트 (서버)
#
# 사용법 (이 파일이 있는 server/ 디렉터리 안에서 실행):
#   bash install.sh           — 기본 설치
#   bash install.sh --dev     — 개발 의존성 포함 (pytest 등)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV=0

for arg in "$@"; do
  case $arg in
    --dev) DEV=1 ;;
  esac
done

echo "=== maas-server-sdk 설치 ==="

if [ "$DEV" -eq 1 ]; then
  pip install -e "${SCRIPT_DIR}[dev]"
else
  pip install -e "${SCRIPT_DIR}"
fi

echo "설치 완료.  from maas_server import MaasServer, RpcContext"
echo "사용법: README.md"
