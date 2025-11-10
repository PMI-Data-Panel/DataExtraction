"""검색 API 라우터 - Celery + Redis 통합"""
import logging
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from opensearchpy import OpenSearch
import os
import redis
import json

from celery.result import AsyncResult
from redis_celery.celery_app import celery_app

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/search",
    tags=["Search"]
)

# ⚠️ 타임아웃 설정
DEFAULT_OS_TIMEOUT = 10


# ==================== Pydantic Models ====================

class NLSearchRequest(BaseModel):
    """자연어 기반 검색 요청"""
    query: str = Field(..., description="자연어 쿼리 (예: '30대 사무직 300명 데이터 보여줘')")
    index_name: str = Field(default="*", description="검색할 인덱스 이름 (기본값: 전체 인덱스 '*')")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")


class SearchResult(BaseModel):
    """검색 결과 항목"""
    user_id: str
    score: float
    timestamp: Optional[str] = None
    demographic_info: Optional[Dict[str, Any]] = None
    qa_pairs: Optional[List[Dict[str, Any]]] = None
    matched_qa_pairs: Optional[List[Dict[str, Any]]] = None
    highlights: Optional[Dict[str, Any]] = None
    index: Optional[str] = None  # ⭐ 인덱스 정보 추가


class SearchResponse(BaseModel):
    """검색 응답"""
    query: str
    total_hits: int
    max_score: Optional[float]
    results: List[SearchResult]
    query_analysis: Optional[Dict[str, Any]] = None
    search_summary: Optional[Dict[str, Any]] = None  # ⭐ 인덱스별 통계 추가
    took_ms: int


# ==================== Redis Helper ====================

def get_redis_client() -> redis.StrictRedis:
    """Redis 연결 객체 반환"""
    REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    CACHE_DB = int(os.getenv('CACHE_DB', '2'))
    
    try:
        r = redis.StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=CACHE_DB,
            decode_responses=True
        )
        r.ping()
        return r
    except Exception as e:
        logger.error(f"Redis 연결 실패: {e}")
        raise HTTPException(
            status_code=503,
            detail="Redis 서버에 연결할 수 없습니다."
        )


# ==================== API Endpoints ====================

@router.get("/", summary="Search API 상태")
def search_root():
    """Search API 기본 정보"""
    return {
        "message": "Search API 실행 중",
        "version": "2.0 (Celery + Redis)",
        "endpoints": [
            "/search/nl-async",
            "/search/status/{task_id}",
            "/search/scroll/{task_id}"
        ]
    }


@router.post("/nl-async", response_model=Dict[str, Any], summary="자연어 검색 (비동기)")
async def search_natural_language_async(request: NLSearchRequest):
    """
    🚀 전체 인덱스 병렬 하이브리드 검색 (Celery Worker에 위임)
    
    - welcome_1st, welcome_2nd, survey_25_* (30개) 동시 검색
    - 각 인덱스마다 OpenSearch + Qdrant 병렬 실행
    - RRF 결합 후 Redis 캐싱
    
    Returns:
        task_id: Celery Task ID
        status_url: 작업 상태 조회 URL
        scroll_url: 무한 스크롤 URL
    """
    from redis_celery.tasks.search_tasks import parallel_hybrid_search_all
    
    try:
        # Celery 태스크 비동기 호출
        task = parallel_hybrid_search_all.delay(
            query=request.query,
            index_name=request.index_name or "*",
            size=100,  # 충분한 결과 확보 (Redis에 캐싱됨)
            use_vector_search=request.use_vector_search,
        )
        
        logger.info(f"🚀 검색 작업 시작: task_id={task.id}, query='{request.query}'")
        
        return {
            "message": "병렬 검색 시작",
            "task_id": task.id,
            "status_url": f"/search/status/{task.id}",
            "scroll_url": f"/search/scroll/{task.id}?offset=0&limit=20"
        }
        
    except Exception as e:
        logger.error(f"❌ 검색 작업 시작 실패: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"검색 작업 시작 실패: {str(e)}"
        )


@router.get("/status/{task_id}", response_model=Dict[str, Any], summary="Celery 작업 상태 조회")
async def get_task_status(task_id: str):
    """
    클라이언트가 이 엔드포인트를 폴링하여 작업 상태 확인
    
    상태:
    - PENDING: 작업 대기 중
    - STARTED: 작업 실행 중
    - SUCCESS: 작업 완료
    - FAILURE: 작업 실패
    """
    try:
        task = AsyncResult(task_id, app=celery_app)
        
        response = {
            'task_id': task_id,
            'state': task.state,
            'ready': task.ready(),
            'successful': task.successful() if task.ready() else None,
        }
        
        if task.ready():
            if task.state == 'SUCCESS':
                result = task.result
                response['result'] = {
                    'status': result.get('status'),
                    'query': result.get('query'),
                    'total_hits': result.get('total_hits'),
                    'max_score': result.get('max_score'),
                    'took_ms': result.get('took_ms'),
                    'search_summary': result.get('search_summary'),
                    'scroll_url': f"/search/scroll/{task_id}?offset=0&limit=20"
                }
            elif task.state == 'FAILURE':
                error_info = task.result if isinstance(task.result, dict) else {'error': str(task.result)}
                response['result'] = {
                    'status': 'failed',
                    'error': error_info.get('error', str(task.result)),
                    'error_type': error_info.get('error_type', 'UnknownError')
                }
        else:
            response['message'] = f"작업 진행 중 ({task.state})"
        
        return response
        
    except Exception as e:
        logger.error(f"❌ 작업 상태 조회 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"작업 상태 조회 실패: {str(e)}"
        )


@router.get("/scroll/{task_id}", response_model=SearchResponse, summary="무한 스크롤: RRF 결과 페이징 조회")
async def get_scrolled_results(
    task_id: str,
    offset: int = Query(0, ge=0, description="시작 위치 (0부터 시작)"),
    limit: int = Query(20, ge=1, le=100, description="반환할 결과 개수 (1~100)"),
    r: redis.StrictRedis = Depends(get_redis_client)
):
    """
    Redis에 캐싱된 RRF 결과를 offset/limit 방식으로 조회
    
    사용 예시:
    - 첫 페이지: GET /search/scroll/{task_id}?offset=0&limit=20
    - 두 번째 페이지: GET /search/scroll/{task_id}?offset=20&limit=20
    - 세 번째 페이지: GET /search/scroll/{task_id}?offset=40&limit=20
    
    Returns:
        SearchResponse: 검색 결과 (RRF 순서 유지)
    """
    try:
        id_list_key = f"task:{task_id}:ids"
        
        # 1. Redis에서 전체 ID 목록 길이 확인
        total_hits = r.llen(id_list_key)
        
        if total_hits == 0:
            raise HTTPException(
                status_code=404,
                detail="검색 결과가 만료되었거나 존재하지 않습니다. 검색을 다시 실행해주세요."
            )
        
        # 2. offset/limit으로 user_id 추출
        end_index = offset + limit - 1
        user_ids = r.lrange(id_list_key, offset, end_index)
        
        if not user_ids:
            # 끝까지 스크롤한 경우
            return SearchResponse(
                query=f"Task {task_id}",
                total_hits=total_hits,
                max_score=0.0,
                results=[],
                took_ms=0
            )
        
        # 3. MGET으로 상세 데이터 일괄 조회
        data_keys = [f"task:{task_id}:data:{uid}" for uid in user_ids]
        detailed_results_json = r.mget(data_keys)
        
        results: List[SearchResult] = []
        max_score = 0.0
        
        for item_json in detailed_results_json:
            if item_json:
                try:
                    data = json.loads(item_json)
                    result = SearchResult(**data)
                    results.append(result)
                    max_score = max(max_score, result.score)
                except json.JSONDecodeError as e:
                    logger.error(f"Redis 데이터 파싱 오류: {e}")
                    continue
        
        logger.info(f"📄 페이징 조회: task_id={task_id}, offset={offset}, limit={limit}, returned={len(results)}/{total_hits}")
        
        # 4. 응답 구성
        return SearchResponse(
            query=f"Task {task_id} (Offset: {offset}, Limit: {limit})",
            total_hits=total_hits,
            max_score=max_score,
            results=results,
            query_analysis={
                "note": "페이징 데이터는 전체 분석 결과를 포함하지 않습니다.",
                "pagination": {
                    "offset": offset,
                    "limit": limit,
                    "has_more": (offset + limit) < total_hits
                }
            },
            took_ms=0
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 페이징 조회 실패: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"페이징 조회 실패: {str(e)}"
        )


# ==================== 디버깅 및 통계 ====================

@router.get("/stats/{index_name}", summary="인덱스 통계")
async def get_search_stats(
    index_name: str,
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """인덱스 검색 통계 조회"""
    try:
        if not os_client.indices.exists(index=index_name):
            raise HTTPException(
                status_code=404,
                detail=f"인덱스를 찾을 수 없습니다: {index_name}"
            )
        
        stats = os_client.indices.stats(index=index_name)
        count = os_client.count(index=index_name)
        
        return {
            "index_name": index_name,
            "doc_count": count['count'],
            "size_mb": round(stats['_all']['total']['store']['size_in_bytes'] / 1024 / 1024, 2),
            "search_total": stats['_all']['total']['search']['query_total'],
            "search_time_ms": stats['_all']['total']['search']['query_time_in_millis']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ERROR] 통계 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/indices", summary="모든 survey 인덱스 목록")
async def list_survey_indices(
    os_client: OpenSearch = Depends(lambda: router.os_client),
):
    """OpenSearch에서 모든 survey 인덱스 목록 조회"""
    try:
        indices_response = os_client.cat.indices(format='json')
        
        survey_indices = [
            {
                'index': idx['index'],
                'doc_count': idx['docs.count'],
                'size': idx['store.size']
            }
            for idx in indices_response
            if idx['index'].startswith('s_welcome') or idx['index'].startswith('survey_25')
        ]
        
        survey_indices.sort(key=lambda x: x['index'])
        
        return {
            "total_indices": len(survey_indices),
            "indices": survey_indices
        }
        
    except Exception as e:
        logger.error(f"[ERROR] 인덱스 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/cache/{task_id}", summary="Redis 캐시 삭제")
async def delete_cache(
    task_id: str,
    r: redis.StrictRedis = Depends(get_redis_client)
):
    """특정 task의 Redis 캐시 삭제"""
    try:
        # ID 리스트 가져오기
        id_list_key = f"task:{task_id}:ids"
        user_ids = r.lrange(id_list_key, 0, -1)
        
        # 삭제할 키 목록
        keys_to_delete = [id_list_key]
        keys_to_delete.extend([f"task:{task_id}:data:{uid}" for uid in user_ids])
        
        # 일괄 삭제
        deleted_count = 0
        if keys_to_delete:
            deleted_count = r.delete(*keys_to_delete)
        
        logger.info(f"🗑️ 캐시 삭제: task_id={task_id}, deleted={deleted_count}개 키")
        
        return {
            "task_id": task_id,
            "deleted_keys": deleted_count,
            "message": "캐시 삭제 완료"
        }
        
    except Exception as e:
        logger.error(f"❌ 캐시 삭제 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"캐시 삭제 실패: {str(e)}"
        )


# ==================== Qdrant 디버깅 ====================

@router.get("/qdrant/collections", summary="Qdrant 컬렉션 목록")
async def list_qdrant_collections():
    """Qdrant 컬렉션 목록 및 통계"""
    qdrant_client = getattr(router, 'qdrant_client', None)
    
    if not qdrant_client:
        raise HTTPException(
            status_code=503,
            detail="Qdrant 클라이언트가 초기화되지 않았습니다."
        )
    
    try:
        cols = qdrant_client.get_collections()
        items = []
        
        for c in cols.collections:
            try:
                info = qdrant_client.get_collection(c.name)
                items.append({
                    "name": c.name,
                    "vectors_count": getattr(info, 'vectors_count', None),
                    "points_count": getattr(info, 'points_count', None),
                })
            except Exception as e:
                items.append({
                    "name": c.name,
                    "error": str(e)
                })
        
        return {
            "total_collections": len(items),
            "collections": items
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 헬스 체크 ====================

@router.get("/health", summary="검색 서비스 헬스 체크")
async def health_check(
    os_client: OpenSearch = Depends(lambda: router.os_client),
    r: redis.StrictRedis = Depends(get_redis_client)
):
    """검색 서비스 상태 확인"""
    status = {
        "opensearch": "disconnected",
        "redis": "disconnected",
        "qdrant": "disconnected",
        "celery": "unknown"
    }
    
    # OpenSearch
    try:
        if os_client and os_client.ping():
            status["opensearch"] = "connected"
    except Exception as e:
        status["opensearch"] = f"error: {str(e)}"
    
    # Redis
    try:
        r.ping()
        status["redis"] = "connected"
    except Exception as e:
        status["redis"] = f"error: {str(e)}"
    
    # Qdrant
    try:
        qdrant_client = getattr(router, 'qdrant_client', None)
        if qdrant_client:
            qdrant_client.get_collections()
            status["qdrant"] = "connected"
    except Exception as e:
        status["qdrant"] = f"error: {str(e)}"
    
    # Celery
    try:
        inspect = celery_app.control.inspect()
        active_workers = inspect.active()
        if active_workers:
            status["celery"] = f"active ({len(active_workers)} workers)"
        else:
            status["celery"] = "no workers"
    except Exception as e:
        status["celery"] = f"error: {str(e)}"
    
    all_healthy = all(
        s == "connected" or "active" in s
        for s in [status["opensearch"], status["redis"], status["qdrant"], status["celery"]]
    )
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "components": status
    }