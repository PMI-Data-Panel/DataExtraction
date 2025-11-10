# redis_celery/celery_app.py (수정)

from celery import Celery
import os

# Redis 연결 설정 (환경 변수 사용)
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')

# Celery Worker의 환경 변수 CELERY_BROKER_URL, CELERY_RESULT_BACKEND를 따르도록 설정
BROKER_URL = os.getenv('CELERY_BROKER_URL', f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
BACKEND_URL = os.getenv('CELERY_RESULT_BACKEND', f'redis://{REDIS_HOST}:{REDIS_PORT}/1')

celery_app = Celery(
    'dataextraction',
    broker=BROKER_URL,
    backend=BACKEND_URL,
)

# 💡 수정: 외부 설정 파일 (celeryconfig.py)에서 설정을 로드
celery_app.config_from_object('redis_celery.celeryconfig')

# 💡 Task 자동 발견 설정 (유지)
celery_app.autodiscover_tasks(['redis_celery.tasks'])


if __name__ == '__main__':
    celery_app.start()