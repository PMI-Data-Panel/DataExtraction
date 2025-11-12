# Python 3.11을 베이스 이미지로 사용 (slim 버전 대신 일반 버전 사용)
FROM python:3.11

# 작업 디렉토리 설정
WORKDIR /app

# 의존성 설치
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 프로젝트 파일 복사
COPY . /app

# FastAPI 서버 실행
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]