# 사용할 Python 기본 이미지 지정 (경량화된 slim 버전 사용)
FROM python:3.11-slim

# 환경 변수 설정
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /app

# 1. Python 모듈 경로 추가: Celery가 /app에서 모듈을 찾도록 보장
ENV PYTHONPATH=/app

# 2. 의존성 파일 복사 및 설치
# 주의: requirements.txt는 /app 아래에 있어야 합니다.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && pip cache purge

# 3. KURE/V1 모델 캐시 경로 설정 (매우 중요)
# 모델이 로컬에 저장될 위치를 지정합니다.
ENV SENTENCE_TRANSFORMERS_HOME=/app/.cache/models

# 4. 모델 사전 다운로드 (모델 로딩 시간 0초 만들기)
RUN mkdir -p ${SENTENCE_TRANSFORMERS_HOME} && \
    python -c "from sentence_transformers import SentenceTransformer; \
            # KURE/V1 모델을 강제로 다운로드합니다.
            SentenceTransformer('nlpai-lab/KURE-v1')"

# 5. 나머지 프로젝트 파일 복사 (전체 프로젝트를 /app으로 복사)
# celery_app.py가 포함된 redis_celery 디렉토리도 함께 복사됩니다.
COPY . /app/

# Celery 워커 시작 명령어 수정: 
# 모듈 경로를 'redis_celery.celery_app'으로 변경합니다.
CMD ["celery", "-A", "redis_celery.celery_app", "worker", "--loglevel=info", "-P", "fork", "--autoscale=10,2"]