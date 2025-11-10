# api/celery_api_improved.py
"""
🚀 개선된 Celery API

주요 개선 사항:
1. 페이징 지원 (Redis 기반)
2. Task 상태 상세 조회
3. 결과 스트리밍
4. 에러 핸들링 강화
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import logging
import json
from redis import StrictRedis
import os

from redis_celery.celery_app import celery_app
from celery.result import AsyncResult

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/celery",
    tags=["Celery Tasks"]
)


# ==========================================
# 📌 Request/Response Models
# ==========================================

class AsyncSearchRequest(BaseModel):
    """비동기 검색 요청"""
    query: str = Field(..., description="검색 쿼리")
    index_name: str = Field(default="*", description="인덱스 이름")
    size: int = Field(default=10, ge=1, le=100, description="결과 개수")
    use_vector_search: bool = Field(default=True, description="벡터 검색 사용 여부")


class AsyncSearchResponse(BaseModel):
    """비동기 검색 응답"""
    task_id: str
    status: str
    message: str
    poll_url: str  # 상태 조회 URL


class TaskStatusResponse(BaseModel):
    """Task 상태 응답"""
    task_id: str
    status: str
    progress: Optional[Dict[str, Any]] = None  # 진행률 정보
    result: Optional[Dict] = None
    error: Optional[str] = None
    traceback: Optional[str] = None


class PaginatedResultsResponse(BaseModel):
    """페이징된 결과 응답"""
    task_id: str
    query: str
    total_hits: int
    page: int
    page_size: int
    total_pages: int
    results: List[Dict]


# ==========================================
# 📌 비동기 검색 시작
# ==========================================

@router.post("/search/async", response_model=AsyncSearchResponse, summary="비동기 검색 시작")
async def start_async_search(request: AsyncSearchRequest):
    """
    비동기 검색 Task를 시작하고 Task ID를 반환합니다.
    
    **개선 사항:**
    - Task ID로 실시간 상태 조회 가능
    - 페이징된 결과 조회 지원
    
    **응답 예시:**
    ```json
    {
      "task_id": "abc-123",
      "status": "pending",
      "message": "검색 Task가 시작되었습니다",
      "poll_url": "/celery/task/abc-123"
    }
    ```
    """
    try:
        # 🔥 개선된 Orchestrator Task 실행
        from redis_celery.tasks.search_tasks import parallel_hybrid_search_orchestrator
        
        task = parallel_hybrid_search_orchestrator.apply_async(
            kwargs={
                'query': request.query,
                'index_name': request.index_name,
                'size': request.size,
                'use_vector_search': request.use_vector_search,
            },
            expires=300,  # 5분 후 만료
        )
        
        logger.info(f"🚀 비동기 검색 Task 시작: task_id={task.id}, query='{request.query}'")
        
        return AsyncSearchResponse(
            task_id=task.id,
            status="pending",
            message=f"검색 Task가 시작되었습니다. {len(request.query.split())}개 키워드 분석 중...",
            poll_url=f"/celery/task/{task.id}"
        )
    
    except Exception as e:
        logger.error(f"❌ 비동기 검색 시작 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 📌 Task 상태 조회 (실시간)
# ==========================================

@router.get("/task/{task_id}", response_model=TaskStatusResponse, summary="Task 상태 조회")
async def get_task_status_endpoint(task_id: str):
    """
    Task ID로 검색 Task의 상태와 결과를 조회합니다.
    
    **상태 종류:**
    - `PENDING`: Task가 대기 중 (Worker 할당 대기)
    - `STARTED`: Task가 실행 중
    - `SUCCESS`: Task가 성공적으로 완료됨
    - `FAILURE`: Task가 실패함
    - `RETRY`: Task가 재시도 중
    
    **진행률 정보:**
    - Chord 진행률 (예: 32개 중 15개 완료)
    - 예상 완료 시간
    """
    try:
        result = AsyncResult(task_id, app=celery_app)
        
        # 상태 정보
        status_info = {
            'task_id': task_id,
            'status': result.state,
        }
        
        # SUCCESS: 결과 반환
        if result.state == 'SUCCESS':
            status_info['result'] = result.result
            
        # FAILURE: 에러 반환
        elif result.state == 'FAILURE':
            status_info['error'] = str(result.info)
            status_info['traceback'] = result.traceback
            
        # PROGRESS: 진행률 반환 (Chord 진행률)
        elif result.state == 'STARTED':
            # Celery의 메타데이터에서 진행률 조회
            meta = result.info or {}
            if isinstance(meta, dict):
                status_info['progress'] = {
                    'completed_tasks': meta.get('completed', 0),
                    'total_tasks': meta.get('total', 0),
                    'message': meta.get('message', '검색 실행 중...')
                }
        
        return TaskStatusResponse(**status_info)
    
    except Exception as e:
        logger.error(f"❌ Task 상태 조회 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 📌 페이징된 결과 조회
# ==========================================

@router.get("/results/{task_id}", response_model=PaginatedResultsResponse, summary="페이징된 결과 조회")
async def get_paginated_results(
    task_id: str,
    page: int = Query(1, ge=1, description="페이지 번호 (1부터 시작)"),
    page_size: int = Query(10, ge=1, le=100, description="페이지 크기")
):
    """
    완료된 Task의 결과를 페이징하여 조회합니다.
    
    **Redis 기반 페이징:**
    - 전체 결과는 Redis에 캐싱
    - user_id 리스트로 페이징 지원
    - TTL: 1시간
    
    **사용 예시:**
    ```
    GET /celery/results/abc-123?page=1&page_size=10
    GET /celery/results/abc-123?page=2&page_size=10
    ```
    """
    try:
        # 1. Task 완료 확인
        result = AsyncResult(task_id, app=celery_app)
        
        if result.state != 'SUCCESS':
            raise HTTPException(
                status_code=400,
                detail=f"Task가 아직 완료되지 않았습니다. 현재 상태: {result.state}"
            )
        
        # 2. Redis에서 페이징 정보 조회
        redis_client = StrictRedis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            db=int(os.getenv('CACHE_DB', '2')),
            decode_responses=True
        )
        
        id_list_key = f"task:{task_id}:ids"
        
        # 전체 user_id 개수
        total_hits = redis_client.llen(id_list_key)
        
        if total_hits == 0:
            raise HTTPException(
                status_code=404,
                detail="결과를 찾을 수 없습니다. 캐시가 만료되었을 수 있습니다."
            )
        
        # 페이징 계산
        total_pages = (total_hits + page_size - 1) // page_size
        
        if page > total_pages:
            raise HTTPException(
                status_code=400,
                detail=f"페이지 번호가 범위를 초과했습니다. 최대 페이지: {total_pages}"
            )
        
        # 3. 페이지에 해당하는 user_id 조회
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size - 1
        
        user_ids = redis_client.lrange(id_list_key, start_idx, end_idx)
        
        # 4. 각 user_id의 상세 정보 조회
        results = []
        for user_id in user_ids:
            data_key = f"task:{task_id}:data:{user_id}"
            data_json = redis_client.get(data_key)
            
            if data_json:
                data = json.loads(data_json)
                results.append(data)
        
        # 5. 원본 쿼리 정보 (Task result에서)
        task_result = result.result or {}
        query = task_result.get('query', '')
        
        logger.info(f"✅ 페이징 조회: task_id={task_id}, page={page}/{total_pages}, results={len(results)}건")
        
        return PaginatedResultsResponse(
            task_id=task_id,
            query=query,
            total_hits=total_hits,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            results=results
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 페이징 조회 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 📌 Task 취소
# ==========================================

@router.delete("/task/{task_id}", summary="Task 취소")
async def cancel_task(task_id: str):
    """
    실행 중인 Task를 취소합니다.
    
    ⚠️ **주의사항:**
    - 이미 시작된 Task는 즉시 중단되지 않을 수 있음
    - Worker가 Task를 완료하기 전에 취소 신호를 받아야 함
    - Chord의 경우 일부 Task만 취소될 수 있음
    """
    try:
        result = AsyncResult(task_id, app=celery_app)
        result.revoke(terminate=True)
        
        logger.info(f"🛑 Task 취소: task_id={task_id}")
        
        return {
            "task_id": task_id,
            "status": "revoked",
            "message": "Task가 취소되었습니다."
        }
    
    except Exception as e:
        logger.error(f"❌ Task 취소 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 📌 Task 통계
# ==========================================

@router.get("/stats", summary="Celery Task 통계")
async def get_celery_stats():
    """
    Celery Worker 및 Task 통계를 조회합니다.
    
    **통계 정보:**
    - Active Workers
    - Pending Tasks
    - Active Tasks
    - Completed Tasks (1시간 내)
    """
    try:
        # Celery Inspect
        from celery import current_app
        inspect = current_app.control.inspect()
        
        # Worker 정보
        active_workers = inspect.active()
        stats = inspect.stats()
        
        # Task 통계
        active_tasks = inspect.active()
        scheduled_tasks = inspect.scheduled()
        
        # Redis에서 완료된 Task 개수 조회
        redis_client = StrictRedis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            db=int(os.getenv('CACHE_DB', '2')),
            decode_responses=True
        )
        
        completed_tasks_count = len(redis_client.keys("task:*:ids"))
        
        return {
            "workers": {
                "active": len(active_workers or {}),
                "details": active_workers
            },
            "tasks": {
                "active": sum(len(tasks) for tasks in (active_tasks or {}).values()),
                "scheduled": sum(len(tasks) for tasks in (scheduled_tasks or {}).values()),
                "completed_cached": completed_tasks_count
            },
            "stats": stats
        }
    
    except Exception as e:
        logger.error(f"❌ 통계 조회 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))