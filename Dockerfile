# Python 3.11을 베이스 이미지로 사용
FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /app

# pip 타임아웃 설정 (10분 = 600초)
# 로컬에서 10-20분 걸리므로 서버에서도 충분한 시간 확보
RUN pip config set global.timeout 1800 && \
    pip config set global.retries 10 && \
    pip config set global.default-timeout 1800 && \
    pip install --upgrade pip

# 의존성 설치
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 프로젝트 파일 복사
COPY . /app

# 모델 다운로드는 런타임으로 이동 (빌드 시점 다운로드 제거)
# RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('nlpai-lab/KURE-v1')"

# FastAPI 서버 실행
CMD ["uvicorn", "api.main_api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]