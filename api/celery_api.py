"""
api/celery_api.py
Celery Task 실행 및 상태 조회 API
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import logging

from redis_celery.tasks.search_tasks import search_with_rrf_task, get_task_status

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/celery",
    tags=["Celery Tasks"]
)


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


class TaskStatusResponse(BaseModel):
    """Task 상태 응답"""
    task_id: str
    status: str
    result: Optional[dict] = None
    traceback: Optional[str] = None


@router.post("/search/async", response_model=AsyncSearchResponse, summary="비동기 검색 시작")
async def start_async_search(request: AsyncSearchRequest):
    """
    비동기 검색 Task를 시작하고 Task ID를 반환합니다.
    
    - Task ID로 `/celery/task/{task_id}` 엔드포인트에서 상태 조회 가능
    """
    try:
        # Celery Task 실행
        task = search_with_rrf_task.apply_async(
            kwargs={
                'query': request.query,
                'index_name': request.index_name,
                'size': request.size,
                'use_vector_search': request.use_vector_search,
            },
            # 옵션: 우선순위, 만료 시간 등
            # priority=5,
            # expires=300,
        )
        
        logger.info(f"🚀 비동기 검색 Task 시작: task_id={task.id}, query='{request.query}'")
        
        return AsyncSearchResponse(
            task_id=task.id,
            status="pending",
            message=f"검색 Task가 시작되었습니다. Task ID: {task.id}"
        )
    
    except Exception as e:
        logger.error(f"❌ 비동기 검색 시작 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/task/{task_id}", response_model=TaskStatusResponse, summary="Task 상태 조회")
async def get_task_status_endpoint(task_id: str):
    """
    Task ID로 검색 Task의 상태와 결과를 조회합니다.
    
    **상태 종류:**
    - `PENDING`: Task가 대기 중
    - `STARTED`: Task가 실행 중
    - `SUCCESS`: Task가 성공적으로 완료됨
    - `FAILURE`: Task가 실패함
    - `RETRY`: Task가 재시도 중
    """
    try:
        status_info = get_task_status(task_id)
        
        return TaskStatusResponse(
            task_id=status_info['task_id'],
            status=status_info['status'],
            result=status_info.get('result'),
            traceback=status_info.get('traceback'),
        )
    
    except Exception as e:
        logger.error(f"❌ Task 상태 조회 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/task/{task_id}", summary="Task 취소")
async def cancel_task(task_id: str):
    """
    실행 중인 Task를 취소합니다.
    
    ⚠️ 주의: 이미 시작된 Task는 즉시 중단되지 않을 수 있습니다.
    """
    try:
        from celery.result import AsyncResult
        from redis_celery.celery_app import celery_app
        
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