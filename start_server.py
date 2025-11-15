"""
FastAPI 서버 시작 스크립트

포트 충돌을 자동으로 감지하고 다른 포트를 사용합니다.
"""

import socket
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_port_in_use(port: int) -> bool:
    """포트가 사용 중인지 확인"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            return False
        except OSError:
            return True


def find_available_port(start_port: int = 8000, max_attempts: int = 10) -> int:
    """사용 가능한 포트 찾기"""
    for port in range(start_port, start_port + max_attempts):
        if not is_port_in_use(port):
            return port
    raise RuntimeError(f"포트 {start_port}~{start_port + max_attempts - 1} 범위에서 사용 가능한 포트를 찾을 수 없습니다.")


def main():
    """서버 시작"""
    logger.info("=" * 60)
    logger.info("🚀 FastAPI 서버 시작")
    logger.info("=" * 60)

    # 사용 가능한 포트 찾기
    default_port = 8000

    if is_port_in_use(default_port):
        logger.warning(f"⚠️ 포트 {default_port}이(가) 이미 사용 중입니다.")
        try:
            available_port = find_available_port(default_port + 1)
            logger.info(f"✅ 대체 포트 {available_port}을(를) 사용합니다.")
            port = available_port
        except RuntimeError as e:
            logger.error(f"❌ {e}")
            logger.info("\n해결 방법:")
            logger.info("1. 기존 프로세스 종료:")
            logger.info("   - Windows: tasklist | findstr python")
            logger.info("             taskkill /F /PID <PID>")
            logger.info("2. 또는 다른 포트 수동 지정:")
            logger.info("   - python -m uvicorn main:app --host 0.0.0.0 --port 8001")
            sys.exit(1)
    else:
        port = default_port
        logger.info(f"✅ 포트 {port}을(를) 사용합니다.")

    # uvicorn 실행
    logger.info(f"\n서버 시작: http://localhost:{port}")
    logger.info(f"API 문서: http://localhost:{port}/docs")
    logger.info("=" * 60 + "\n")

    try:
        subprocess.run([
            sys.executable, "-m", "uvicorn",
            "main:app",
            "--host", "0.0.0.0",
            "--port", str(port)
            # reload 옵션 제거: 파일 변경 감지 비활성화
        ])
    except KeyboardInterrupt:
        logger.info("\n\n서버를 종료합니다...")


if __name__ == "__main__":
    main()
