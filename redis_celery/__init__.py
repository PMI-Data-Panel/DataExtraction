# redis_celery/__init__.py
"""
Redis Celery 패키지
"""

from redis_celery.celery_app import celery_app

__all__ = ['celery_app']