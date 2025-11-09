# 사용할 Python 기본 이미지 지정 (경량화된 alpine 버전 사용 권장)
FROM python:3.11

# 환경 변수 설정 (파이썬이 stdout/stderr로 로그를 출력하도록 하여 Docker 로그 시스템과 통합)
ENV PYTHONUNBUFFERED 1

# 컨테이너 내 작업 디렉토리 설정
WORKDIR /app

# 1. 의존성 파일 복사
# 먼저 requirements.txt를 복사하여 캐시를 활용한 빠른 빌드를 유도
COPY requirements.txt /app/

# 2. 파이썬 라이브러리 설치
# Celery 및 Redis 연동에 필요한 라이브러리가 여기에 정의되어 있어야 합니다.
RUN pip install --no-cache-dir -r requirements.txt

# 3. 애플리케이션 코드 복사
# 프로젝트의 모든 코드를 컨테이너의 /app 디렉토리에 복사합니다.
COPY . /app/
