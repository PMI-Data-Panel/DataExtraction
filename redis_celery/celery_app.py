# celery_app.py (간단 버전)
from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

celery_app = Celery(
    'surveypilot',
    broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'),
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Seoul',
    enable_utc=True,
)

# 테스트 작업
@celery_app.task
def test_task():
    return "Celery is working!"