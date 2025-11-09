"""
redis_celery/tasks/search_tasks.py
Celery 검색 태스크
"""
from redis_celery.celery_app import celery_app
import logging
from typing import Dict, Any, Tuple
import os
import time

# 💡 클라이언트 및 코어 로직 임포트
from opensearchpy import OpenSearch
from connectors.data_fetcher import DataFetcher
from api.search_core import execute_hybrid_search
from celery.result import AsyncResult
from celery import signals
from rag_query_analyzer.config import get_config, Config

logger = logging.getLogger(__name__)

# 💡 Worker 프로세스당 한 번만 로드되는 모델 캐시
_embedding_model = None
config: Config = None
config = get_config()

def get_opensearch_client() -> OpenSearch:
    """OpenSearch 클라이언트 초기화"""
    return OpenSearch(
        hosts=[{
            'host': os.getenv('OPENSEARCH_HOST', 'localhost'),
            'port': int(os.getenv('OPENSEARCH_PORT', '9200'))
        }],
        http_auth=(
            os.getenv('OPENSEARCH_USER', 'admin'),
            os.getenv('OPENSEARCH_PASSWORD', 'admin')
        ),
        use_ssl=os.getenv('OPENSEARCH_USE_SSL', 'false').lower() == 'true',
        verify_certs=False,
        timeout=30,
    )


def get_data_fetcher() -> DataFetcher:
    """DataFetcher 초기화 (Celery Worker용 - 동기 전용)"""
    opensearch_client = get_opensearch_client()
    
    # Qdrant 클라이언트 (필요시)
    qdrant_client = None
    try:
        from qdrant_client import QdrantClient
        qdrant_host = os.getenv('QDRANT_HOST')
        if qdrant_host:
            qdrant_client = QdrantClient(host=qdrant_host, port=6333)
    except Exception as e:
        logger.warning(f"Qdrant 클라이언트 초기화 실패: {e}")
    
    return DataFetcher(
        opensearch_client=opensearch_client,
        qdrant_client=qdrant_client,
        async_opensearch_client=None,  # Celery는 동기 전용
    )

def get_embedding_model():
    """임베딩 모델 로드 (캐시된 전역 변수 반환)"""
    global _embedding_model
    return _embedding_model

@signals.worker_process_init.connect
def setup_model_on_worker_init(**kwargs):
    """Worker 프로세스가 시작될 때 임베딩 모델을 로드하여 캐시합니다."""
    global _embedding_model
    if _embedding_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            # 모델 로딩은 시간이 오래 걸리므로 Worker 프로세스 시작 시 한 번만 수행
            _embedding_model = SentenceTransformer(config.EMBEDDING_MODEL)
            logger.info("✅ Worker 프로세스 시작: 임베딩 모델 로드 완료")
        except Exception as e:
            logger.error(f"❌ 임베딩 모델 로드 실패: {e}")
            _embedding_model = None
            
@celery_app.task(
    name='tasks.search_tasks.search_with_rrf_task',
    bind=True,
    max_retries=3,
    default_retry_delay=5,
)


def search_with_rrf_task(self, query: str, index_name: str = "*", size: int = 10, use_vector_search: bool = True):
    """하이브리드 검색 Celery Task: 비동기 로직을 동기적으로 실행합니다."""
    
    logger.info(f"🚀 [Task {self.request.id}] 검색 시작: query='{query}', size={size}")
    
    try:
        # 1. 클라이언트 초기화
        data_fetcher = get_data_fetcher()
        embedding_model = get_embedding_model() if use_vector_search else None
        
        # 2. 핵심 검색 로직 실행 (동기 모드)
        import asyncio
        
        # Celery에서는 비동기를 동기로 실행
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                execute_hybrid_search(
                    query=query,
                    index_name=index_name,
                    size=size,
                    use_vector_search=use_vector_search,
                    data_fetcher=data_fetcher,
                    embedding_model=embedding_model,
                    config=config,
                    is_async=False,  # Celery는 동기 모드
                )
            )
        finally:
            loop.close()
        
        result['status'] = 'completed'
        result['task_id'] = self.request.id
        
        logger.info(f"✅ [Task {self.request.id}] 검색 완료: {result['total_hits']}건")
        return result
        
    except Exception as exc:
        logger.error(f"❌ [Task {self.request.id}] 검색 실패: {exc}", exc_info=True)
        
        # 재시도 로직
        try:
            raise self.retry(exc=exc, countdown=5)
        except self.MaxRetriesExceededError:
            return {
                'status': 'failed',
                'error': str(exc),
                'error_type': type(exc).__name__,
                'query': query,
                'task_id': self.request.id,
            }


# @celery_app.task(name='tasks.search_tasks.get_task_status')
# def get_task_status(task_id: str) -> Dict[str, Any]:
#     """
#     Task 상태 조회
    
#     Args:
#         task_id: Task ID
    
#     Returns:
#         Task 상태 정보
#     """
#     from celery.result import AsyncResult
    
#     result = AsyncResult(task_id, app=celery_app)
    
#     return {
#         'task_id': task_id,
#         'status': result.state,
#         'result': result.result if result.ready() else None,
#         'traceback': result.traceback if result.failed() else None,
#     }