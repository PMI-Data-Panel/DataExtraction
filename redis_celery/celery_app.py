# redis_celery/celery_app.py
"""
Celery 앱 설정
"""
from celery import Celery
import os

# Redis 연결 설정
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_DB = os.getenv('REDIS_DB', '0')

REDIS_URL = f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
# Celery Worker의 환경 변수 CELERY_BROKER_URL, CELERY_RESULT_BACKEND를 따르도록 설정
BROKER_URL = os.getenv('CELERY_BROKER_URL', f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
BACKEND_URL = os.getenv('CELERY_RESULT_BACKEND', f'redis://{REDIS_HOST}:{REDIS_PORT}/1')

celery_app = Celery(
    'dataextraction',
    broker=BROKER_URL,
    backend=BACKEND_URL,
)

# Celery 설정
celery_app.conf.update(
    # 직렬화 및 콘텐츠
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # 시간 및 상태 추적
    timezone='Asia/Seoul',
    enable_utc=True,
    task_track_started=True,
    
    # 제한 시간 및 만료
    task_time_limit=300, 
    result_expires=3600, 
)
# Task 자동 발견 설정
celery_app.autodiscover_tasks(['redis_celery.tasks'])


if __name__ == '__main__':
    celery_app.start()